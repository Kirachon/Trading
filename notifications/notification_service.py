"""
Notification Service
Handles Telegram bot integration, email alerts, and risk notifications.
"""
import asyncio
import logging
import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from jinja2 import Template
from telegram import Bot
from telegram.error import TelegramError
import redis.asyncio as redis

# Add shared modules to path
sys.path.append('/app')
from shared.messaging import MessageConsumer, RabbitMQPublisher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NotificationService:
    """Notification service for trading alerts and updates."""
    
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:SecureRabbit2024!@rabbitmq:5672/')
        self.redis_url = os.getenv('REDIS_URL', 'redis://redis:6379')
        
        # Telegram configuration
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.telegram_bot = None
        
        # Email configuration
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_username = os.getenv('SMTP_USERNAME')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.email_from = os.getenv('EMAIL_FROM', self.smtp_username)
        self.email_to = os.getenv('EMAIL_TO', '').split(',')
        
        # Messaging components
        self.alert_consumer = MessageConsumer(self.rabbitmq_url)
        self.fill_consumer = MessageConsumer(self.rabbitmq_url)
        self.signal_consumer = MessageConsumer(self.rabbitmq_url)
        self.portfolio_consumer = MessageConsumer(self.rabbitmq_url)
        self.command_consumer = MessageConsumer(self.rabbitmq_url)
        self.rabbitmq_publisher = RabbitMQPublisher(self.rabbitmq_url)
        
        # Redis for rate limiting and deduplication
        self.redis_client = None
        
        # Notification settings
        self.notification_settings = {
            'telegram_enabled': bool(self.telegram_token and self.telegram_chat_id),
            'email_enabled': bool(self.smtp_username and self.smtp_password and self.email_to),
            'trade_notifications': True,
            'risk_notifications': True,
            'system_notifications': True,
            'daily_summary': True,
            'rate_limit_seconds': 60  # Minimum seconds between similar notifications
        }
        
        # Control flags
        self.running = False
        
        # Email templates
        self.email_templates = self.load_email_templates()
        
    def load_email_templates(self) -> Dict[str, Template]:
        """Load email templates."""
        templates = {
            'trade_alert': Template("""
            <h2>Trading Alert</h2>
            <p><strong>Type:</strong> {{ alert_type }}</p>
            <p><strong>Strategy:</strong> {{ strategy_id }}</p>
            <p><strong>Symbol:</strong> {{ symbol }}</p>
            <p><strong>Side:</strong> {{ side }}</p>
            <p><strong>Price:</strong> ${{ price }}</p>
            <p><strong>Quantity:</strong> {{ quantity }}</p>
            <p><strong>Time:</strong> {{ timestamp }}</p>
            {% if message %}
            <p><strong>Message:</strong> {{ message }}</p>
            {% endif %}
            """),
            
            'risk_alert': Template("""
            <h2>Risk Management Alert</h2>
            <p><strong>Alert Type:</strong> {{ alert_type }}</p>
            <p><strong>Severity:</strong> {{ severity }}</p>
            <p><strong>Message:</strong> {{ message }}</p>
            <p><strong>Time:</strong> {{ timestamp }}</p>
            {% if details %}
            <h3>Details:</h3>
            <ul>
            {% for key, value in details.items() %}
            <li><strong>{{ key }}:</strong> {{ value }}</li>
            {% endfor %}
            </ul>
            {% endif %}
            """),
            
            'daily_summary': Template("""
            <h2>Daily Trading Summary</h2>
            <p><strong>Date:</strong> {{ date }}</p>
            <p><strong>Total P&L:</strong> ${{ total_pnl }}</p>
            <p><strong>Total Trades:</strong> {{ total_trades }}</p>
            <p><strong>Win Rate:</strong> {{ win_rate }}%</p>
            <p><strong>Active Positions:</strong> {{ active_positions }}</p>
            
            <h3>Strategy Performance:</h3>
            <table border="1" style="border-collapse: collapse;">
            <tr><th>Strategy</th><th>Trades</th><th>P&L</th><th>Win Rate</th></tr>
            {% for strategy in strategies %}
            <tr>
            <td>{{ strategy.name }}</td>
            <td>{{ strategy.trades }}</td>
            <td>${{ strategy.pnl }}</td>
            <td>{{ strategy.win_rate }}%</td>
            </tr>
            {% endfor %}
            </table>
            """)
        }
        return templates
    
    async def initialize(self):
        """Initialize the notification service."""
        try:
            # Connect to messaging systems
            await self.alert_consumer.connect()
            await self.fill_consumer.connect()
            await self.signal_consumer.connect()
            await self.portfolio_consumer.connect()
            await self.command_consumer.connect()
            await self.rabbitmq_publisher.connect()
            
            # Connect to Redis
            self.redis_client = redis.from_url(self.redis_url)
            
            # Initialize Telegram bot
            if self.notification_settings['telegram_enabled']:
                self.telegram_bot = Bot(token=self.telegram_token)
                try:
                    bot_info = await self.telegram_bot.get_me()
                    logger.info(f"Telegram bot initialized: {bot_info.username}")
                except TelegramError as e:
                    logger.error(f"Failed to initialize Telegram bot: {e}")
                    self.notification_settings['telegram_enabled'] = False
            
            # Test email configuration
            if self.notification_settings['email_enabled']:
                try:
                    await self.test_email_connection()
                    logger.info("Email configuration verified")
                except Exception as e:
                    logger.error(f"Email configuration failed: {e}")
                    self.notification_settings['email_enabled'] = False
            
            logger.info("Notification Service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Notification Service: {e}")
            raise
    
    async def test_email_connection(self):
        """Test email server connection."""
        try:
            server = aiosmtplib.SMTP(hostname=self.smtp_server, port=self.smtp_port)
            await server.connect()
            await server.starttls()
            await server.login(self.smtp_username, self.smtp_password)
            await server.quit()
        except Exception as e:
            raise Exception(f"Email connection test failed: {e}")
    
    async def should_send_notification(self, notification_type: str, content_hash: str) -> bool:
        """Check if notification should be sent based on rate limiting."""
        try:
            rate_limit_key = f"notification_rate_limit:{notification_type}:{content_hash}"
            
            # Check if we've sent this notification recently
            last_sent = await self.redis_client.get(rate_limit_key)
            
            if last_sent:
                return False
            
            # Set rate limit
            await self.redis_client.setex(
                rate_limit_key,
                self.notification_settings['rate_limit_seconds'],
                datetime.now().isoformat()
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking notification rate limit: {e}")
            return True  # Default to sending if rate limit check fails
    
    async def send_telegram_message(self, message: str, parse_mode: str = 'HTML'):
        """Send message via Telegram."""
        try:
            if not self.notification_settings['telegram_enabled']:
                return False
            
            await self.telegram_bot.send_message(
                chat_id=self.telegram_chat_id,
                text=message,
                parse_mode=parse_mode
            )
            
            logger.debug("Telegram message sent successfully")
            return True
            
        except TelegramError as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False
    
    async def send_email(self, subject: str, html_content: str, text_content: str = None):
        """Send email notification."""
        try:
            if not self.notification_settings['email_enabled']:
                return False
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.email_from
            msg['To'] = ', '.join(self.email_to)
            
            # Add text content
            if text_content:
                text_part = MIMEText(text_content, 'plain')
                msg.attach(text_part)
            
            # Add HTML content
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            # Send email
            server = aiosmtplib.SMTP(hostname=self.smtp_server, port=self.smtp_port)
            await server.connect()
            await server.starttls()
            await server.login(self.smtp_username, self.smtp_password)
            await server.send_message(msg)
            await server.quit()
            
            logger.debug("Email sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False
    
    async def process_trade_notification(self, fill_event: Dict[str, Any]):
        """Process trade fill notification."""
        try:
            if not self.notification_settings['trade_notifications']:
                return
            
            # Create content hash for deduplication
            content_hash = f"{fill_event.get('order_id', '')}_{fill_event.get('timestamp', '')}"
            
            if not await self.should_send_notification('trade', content_hash):
                return
            
            # Format trade information
            symbol = fill_event.get('symbol', 'Unknown')
            side = fill_event.get('side', 'Unknown')
            quantity = fill_event.get('quantity', 0)
            price = fill_event.get('price', 0)
            strategy_id = fill_event.get('strategy_id', 'Unknown')
            timestamp = datetime.fromtimestamp(fill_event.get('timestamp', 0) / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
            # Telegram message
            telegram_message = f"""
🔔 <b>Trade Executed</b>

📈 <b>Strategy:</b> {strategy_id}
💱 <b>Symbol:</b> {symbol}
📊 <b>Side:</b> {side.upper()}
💰 <b>Price:</b> ${price:.4f}
📦 <b>Quantity:</b> {quantity:.6f}
🕐 <b>Time:</b> {timestamp}
💵 <b>Value:</b> ${float(quantity) * float(price):.2f}
            """
            
            # Send Telegram notification
            await self.send_telegram_message(telegram_message)
            
            # Email notification
            email_subject = f"Trade Alert: {side.upper()} {symbol}"
            email_content = self.email_templates['trade_alert'].render(
                alert_type='Trade Execution',
                strategy_id=strategy_id,
                symbol=symbol,
                side=side.upper(),
                price=f"{price:.4f}",
                quantity=f"{quantity:.6f}",
                timestamp=timestamp
            )
            
            await self.send_email(email_subject, email_content)
            
            logger.info(f"Trade notification sent: {side} {symbol} at {price}")
            
        except Exception as e:
            logger.error(f"Error processing trade notification: {e}")
    
    async def process_risk_alert(self, alert: Dict[str, Any]):
        """Process risk management alert."""
        try:
            if not self.notification_settings['risk_notifications']:
                return
            
            alert_type = alert.get('type', 'Unknown')
            severity = alert.get('severity', 'info')
            message = alert.get('message', 'No message')
            
            # Create content hash for deduplication
            content_hash = f"{alert_type}_{message}"[:50]
            
            if not await self.should_send_notification('risk', content_hash):
                return
            
            # Determine emoji based on severity
            severity_emojis = {
                'high': '🚨',
                'medium': '⚠️',
                'low': '💡',
                'info': 'ℹ️'
            }
            emoji = severity_emojis.get(severity, 'ℹ️')
            
            # Telegram message
            telegram_message = f"""
{emoji} <b>Risk Alert</b>

🔴 <b>Type:</b> {alert_type}
📊 <b>Severity:</b> {severity.upper()}
💬 <b>Message:</b> {message}
🕐 <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            # Add details if available
            if 'details' in alert:
                telegram_message += "\n\n<b>Details:</b>\n"
                for key, value in alert['details'].items():
                    telegram_message += f"• <b>{key}:</b> {value}\n"
            
            # Send Telegram notification
            await self.send_telegram_message(telegram_message)
            
            # Email notification for high severity alerts
            if severity in ['high', 'medium']:
                email_subject = f"Risk Alert: {alert_type} ({severity.upper()})"
                email_content = self.email_templates['risk_alert'].render(
                    alert_type=alert_type,
                    severity=severity.upper(),
                    message=message,
                    timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    details=alert.get('details', {})
                )
                
                await self.send_email(email_subject, email_content)
            
            logger.info(f"Risk alert sent: {alert_type} ({severity})")
            
        except Exception as e:
            logger.error(f"Error processing risk alert: {e}")
    
    async def process_signal_notification(self, signal: Dict[str, Any]):
        """Process trading signal notification."""
        try:
            # Only send notifications for significant signals
            confidence = signal.get('confidence', 0)
            if confidence < 0.7:  # Only high-confidence signals
                return
            
            strategy_id = signal.get('strategy_id', 'Unknown')
            symbol = signal.get('symbol', 'Unknown')
            side = signal.get('side', 'Unknown')
            price = signal.get('price', 0)
            
            # Create content hash for deduplication
            content_hash = f"{strategy_id}_{symbol}_{side}_{int(price)}"
            
            if not await self.should_send_notification('signal', content_hash):
                return
            
            # Telegram message
            telegram_message = f"""
📡 <b>Trading Signal</b>

🧠 <b>Strategy:</b> {strategy_id}
💱 <b>Symbol:</b> {symbol}
📊 <b>Signal:</b> {side.upper()}
💰 <b>Price:</b> ${price:.4f}
🎯 <b>Confidence:</b> {confidence:.1%}
🕐 <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
            """
            
            await self.send_telegram_message(telegram_message)
            
            logger.debug(f"Signal notification sent: {strategy_id} {side} {symbol}")
            
        except Exception as e:
            logger.error(f"Error processing signal notification: {e}")

    async def send_daily_summary(self):
        """Send daily trading summary."""
        try:
            if not self.notification_settings['daily_summary']:
                return

            # Get daily statistics (simplified - would query actual data)
            today = datetime.now().date()

            # Mock data for demonstration
            daily_stats = {
                'date': today.strftime('%Y-%m-%d'),
                'total_pnl': 125.50,
                'total_trades': 8,
                'win_rate': 62.5,
                'active_positions': 3,
                'strategies': [
                    {'name': 'MA Crossover', 'trades': 3, 'pnl': 45.20, 'win_rate': 66.7},
                    {'name': 'RSI Strategy', 'trades': 2, 'pnl': 32.10, 'win_rate': 50.0},
                    {'name': 'MACD Strategy', 'trades': 3, 'pnl': 48.20, 'win_rate': 66.7}
                ]
            }

            # Telegram summary
            telegram_message = f"""
📊 <b>Daily Trading Summary</b>
📅 <b>Date:</b> {daily_stats['date']}

💰 <b>Total P&L:</b> ${daily_stats['total_pnl']:.2f}
📈 <b>Total Trades:</b> {daily_stats['total_trades']}
🎯 <b>Win Rate:</b> {daily_stats['win_rate']:.1f}%
📦 <b>Active Positions:</b> {daily_stats['active_positions']}

<b>Strategy Performance:</b>
            """

            for strategy in daily_stats['strategies']:
                telegram_message += f"• {strategy['name']}: {strategy['trades']} trades, ${strategy['pnl']:.2f} P&L\n"

            await self.send_telegram_message(telegram_message)

            # Email summary
            email_subject = f"Daily Trading Summary - {daily_stats['date']}"
            email_content = self.email_templates['daily_summary'].render(**daily_stats)

            await self.send_email(email_subject, email_content)

            logger.info("Daily summary sent")

        except Exception as e:
            logger.error(f"Error sending daily summary: {e}")

    async def process_system_notification(self, event: Dict[str, Any]):
        """Process system event notification."""
        try:
            if not self.notification_settings['system_notifications']:
                return

            event_type = event.get('event_type', 'Unknown')
            service = event.get('service', 'Unknown')
            message = event.get('message', 'No message')
            severity = event.get('severity', 'info')

            # Only send notifications for important system events
            if severity not in ['high', 'medium']:
                return

            # Create content hash for deduplication
            content_hash = f"{event_type}_{service}"

            if not await self.should_send_notification('system', content_hash):
                return

            # Determine emoji based on event type
            event_emojis = {
                'service_start': '🟢',
                'service_stop': '🔴',
                'service_error': '❌',
                'service_warning': '⚠️',
                'connection_lost': '📡',
                'connection_restored': '✅'
            }
            emoji = event_emojis.get(event_type, '🔧')

            # Telegram message
            telegram_message = f"""
{emoji} <b>System Event</b>

🖥️ <b>Service:</b> {service}
📋 <b>Event:</b> {event_type}
💬 <b>Message:</b> {message}
🕐 <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """

            await self.send_telegram_message(telegram_message)

            logger.info(f"System notification sent: {event_type} for {service}")

        except Exception as e:
            logger.error(f"Error processing system notification: {e}")

    async def handle_command(self, command: Dict[str, Any]):
        """Handle notification service commands."""
        try:
            command_type = command.get('type')

            if command_type == 'send_test_notification':
                # Send test notifications
                test_message = "🧪 Test notification from Trading System"
                await self.send_telegram_message(test_message)
                await self.send_email("Test Notification", "<h2>Test Email</h2><p>This is a test email from the trading system.</p>")

            elif command_type == 'send_daily_summary':
                await self.send_daily_summary()

            elif command_type == 'update_settings':
                settings = command.get('settings', {})
                self.notification_settings.update(settings)
                logger.info(f"Notification settings updated: {settings}")

            elif command_type == 'get_settings':
                await self.rabbitmq_publisher.publish(
                    {'settings': self.notification_settings},
                    'notifications.settings'
                )

            else:
                logger.warning(f"Unknown command type: {command_type}")

        except Exception as e:
            logger.error(f"Error handling command: {e}")

    async def schedule_daily_summary(self):
        """Schedule daily summary at end of trading day."""
        try:
            while self.running:
                now = datetime.now()

                # Send summary at 23:00 UTC (end of trading day)
                if now.hour == 23 and now.minute == 0:
                    await self.send_daily_summary()

                    # Wait until next minute to avoid sending multiple times
                    await asyncio.sleep(60)
                else:
                    # Check every minute
                    await asyncio.sleep(60)

        except Exception as e:
            logger.error(f"Error in daily summary scheduler: {e}")

    async def run(self):
        """Main service loop."""
        self.running = True

        try:
            await self.initialize()

            # Start consuming different event types
            alert_task = asyncio.create_task(
                self.alert_consumer.consume(
                    'alerts-notifications',
                    self.process_risk_alert,
                    'trading.alerts'
                )
            )

            fill_task = asyncio.create_task(
                self.fill_consumer.consume(
                    'fills-notifications',
                    self.process_trade_notification,
                    'trading.fills'
                )
            )

            signal_task = asyncio.create_task(
                self.signal_consumer.consume(
                    'signals-notifications',
                    self.process_signal_notification,
                    'trading.signals'
                )
            )

            command_task = asyncio.create_task(
                self.command_consumer.consume(
                    'notification-commands',
                    self.handle_command,
                    'commands.notifications'
                )
            )

            # Start daily summary scheduler
            summary_task = asyncio.create_task(self.schedule_daily_summary())

            logger.info("Notification Service started successfully")

            # Run all tasks
            await asyncio.gather(
                alert_task, fill_task, signal_task, command_task, summary_task,
                return_exceptions=True
            )

        except Exception as e:
            logger.error(f"Error in main service loop: {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Cleanup resources."""
        self.running = False

        try:
            await self.alert_consumer.close()
            await self.fill_consumer.close()
            await self.signal_consumer.close()
            await self.command_consumer.close()
            await self.rabbitmq_publisher.close()

            if self.redis_client:
                await self.redis_client.close()

            logger.info("Notification Service cleanup completed")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    """Main entry point."""
    service = NotificationService()

    try:
        await service.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Service error: {e}")
    finally:
        await service.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
