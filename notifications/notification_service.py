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
from shared.exceptions import (
    NotificationError, TemplateError, DataAccessError,
    RecoveryStrategy, log_exception_context
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DynamicTemplateManager:
    """Manages dynamic notification templates with validation and real data integration."""

    def __init__(self):
        self.templates = {}
        self.template_cache = {}
        self.cache_ttl = 3600  # 1 hour cache TTL
        self.load_default_templates()

    def load_default_templates(self):
        """Load default configurable templates."""
        self.templates = {
            'trade_alert': {
                'email': {
                    'subject': 'Trade Alert: {{ side|upper }} {{ symbol }}',
                    'body': '''
                    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                        <h2 style="color: #2c3e50;">🔔 Trading Alert</h2>
                        <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                            <table style="width: 100%; border-collapse: collapse;">
                                <tr><td style="padding: 8px; font-weight: bold;">Type:</td><td style="padding: 8px;">{{ alert_type }}</td></tr>
                                <tr><td style="padding: 8px; font-weight: bold;">Strategy:</td><td style="padding: 8px;">{{ strategy_id }}</td></tr>
                                <tr><td style="padding: 8px; font-weight: bold;">Symbol:</td><td style="padding: 8px;">{{ symbol }}</td></tr>
                                <tr><td style="padding: 8px; font-weight: bold;">Side:</td><td style="padding: 8px; color: {% if side|lower == 'buy' %}#28a745{% else %}#dc3545{% endif %};">{{ side|upper }}</td></tr>
                                <tr><td style="padding: 8px; font-weight: bold;">Price:</td><td style="padding: 8px;">${{ "%.4f"|format(price) }}</td></tr>
                                <tr><td style="padding: 8px; font-weight: bold;">Quantity:</td><td style="padding: 8px;">{{ "%.6f"|format(quantity) }}</td></tr>
                                <tr><td style="padding: 8px; font-weight: bold;">Value:</td><td style="padding: 8px;">${{ "%.2f"|format(price * quantity) }}</td></tr>
                                <tr><td style="padding: 8px; font-weight: bold;">Time:</td><td style="padding: 8px;">{{ timestamp }}</td></tr>
                            </table>
                            {% if message %}
                            <div style="margin-top: 15px; padding: 10px; background: #e9ecef; border-radius: 4px;">
                                <strong>Message:</strong> {{ message }}
                            </div>
                            {% endif %}
                        </div>
                    </div>
                    '''
                },
                'telegram': '''
🔔 <b>Trade Alert</b>

📈 <b>Strategy:</b> {{ strategy_id }}
💱 <b>Symbol:</b> {{ symbol }}
📊 <b>Side:</b> {{ side|upper }}
💰 <b>Price:</b> ${{ "%.4f"|format(price) }}
📦 <b>Quantity:</b> {{ "%.6f"|format(quantity) }}
💵 <b>Value:</b> ${{ "%.2f"|format(price * quantity) }}
🕐 <b>Time:</b> {{ timestamp }}
{% if message %}

💬 <b>Message:</b> {{ message }}
{% endif %}
                '''
            },

            'risk_alert': {
                'email': {
                    'subject': 'Risk Alert: {{ alert_type }} ({{ severity|upper }})',
                    'body': '''
                    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                        <h2 style="color: #dc3545;">⚠️ Risk Management Alert</h2>
                        <div style="background: {% if severity == 'high' %}#f8d7da{% elif severity == 'medium' %}#fff3cd{% else %}#d1ecf1{% endif %}; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid {% if severity == 'high' %}#dc3545{% elif severity == 'medium' %}#ffc107{% else %}#17a2b8{% endif %};">
                            <table style="width: 100%; border-collapse: collapse;">
                                <tr><td style="padding: 8px; font-weight: bold;">Alert Type:</td><td style="padding: 8px;">{{ alert_type }}</td></tr>
                                <tr><td style="padding: 8px; font-weight: bold;">Severity:</td><td style="padding: 8px; color: {% if severity == 'high' %}#dc3545{% elif severity == 'medium' %}#ffc107{% else %}#17a2b8{% endif %};">{{ severity|upper }}</td></tr>
                                <tr><td style="padding: 8px; font-weight: bold;">Time:</td><td style="padding: 8px;">{{ timestamp }}</td></tr>
                            </table>
                            <div style="margin-top: 15px; padding: 10px; background: rgba(255,255,255,0.7); border-radius: 4px;">
                                <strong>Message:</strong> {{ message }}
                            </div>
                            {% if details %}
                            <div style="margin-top: 15px;">
                                <strong>Details:</strong>
                                <ul style="margin: 10px 0; padding-left: 20px;">
                                {% for key, value in details.items() %}
                                <li><strong>{{ key }}:</strong> {{ value }}</li>
                                {% endfor %}
                                </ul>
                            </div>
                            {% endif %}
                        </div>
                    </div>
                    '''
                },
                'telegram': '''
⚠️ <b>Risk Alert</b>

🚨 <b>Type:</b> {{ alert_type }}
📊 <b>Severity:</b> {{ severity|upper }}
🕐 <b>Time:</b> {{ timestamp }}

💬 <b>Message:</b> {{ message }}

{% if details %}
<b>Details:</b>
{% for key, value in details.items() %}
• <b>{{ key }}:</b> {{ value }}
{% endfor %}
{% endif %}
                '''
            },

            'daily_summary': {
                'email': {
                    'subject': 'Daily Trading Summary - {{ date }}',
                    'body': '''
                    <div style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto;">
                        <h2 style="color: #2c3e50;">📊 Daily Trading Summary</h2>
                        <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
                            <h3 style="color: #495057;">Overview - {{ date }}</h3>
                            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0;">
                                <div style="background: white; padding: 15px; border-radius: 6px; text-align: center;">
                                    <div style="font-size: 24px; font-weight: bold; color: {% if total_pnl >= 0 %}#28a745{% else %}#dc3545{% endif %};">${{ "%.2f"|format(total_pnl) }}</div>
                                    <div style="color: #6c757d;">Total P&L</div>
                                </div>
                                <div style="background: white; padding: 15px; border-radius: 6px; text-align: center;">
                                    <div style="font-size: 24px; font-weight: bold; color: #17a2b8;">{{ total_trades }}</div>
                                    <div style="color: #6c757d;">Total Trades</div>
                                </div>
                                <div style="background: white; padding: 15px; border-radius: 6px; text-align: center;">
                                    <div style="font-size: 24px; font-weight: bold; color: #ffc107;">{{ "%.1f"|format(win_rate) }}%</div>
                                    <div style="color: #6c757d;">Win Rate</div>
                                </div>
                                <div style="background: white; padding: 15px; border-radius: 6px; text-align: center;">
                                    <div style="font-size: 24px; font-weight: bold; color: #6f42c1;">{{ active_positions }}</div>
                                    <div style="color: #6c757d;">Active Positions</div>
                                </div>
                            </div>

                            {% if strategies %}
                            <h3 style="color: #495057; margin-top: 30px;">Strategy Performance</h3>
                            <table style="width: 100%; border-collapse: collapse; background: white; border-radius: 6px; overflow: hidden;">
                                <thead>
                                    <tr style="background: #e9ecef;">
                                        <th style="padding: 12px; text-align: left;">Strategy</th>
                                        <th style="padding: 12px; text-align: center;">Trades</th>
                                        <th style="padding: 12px; text-align: center;">P&L</th>
                                        <th style="padding: 12px; text-align: center;">Win Rate</th>
                                    </tr>
                                </thead>
                                <tbody>
                                {% for strategy in strategies %}
                                    <tr style="border-bottom: 1px solid #dee2e6;">
                                        <td style="padding: 12px;">{{ strategy.name }}</td>
                                        <td style="padding: 12px; text-align: center;">{{ strategy.trades }}</td>
                                        <td style="padding: 12px; text-align: center; color: {% if strategy.pnl >= 0 %}#28a745{% else %}#dc3545{% endif %};">${{ "%.2f"|format(strategy.pnl) }}</td>
                                        <td style="padding: 12px; text-align: center;">{{ "%.1f"|format(strategy.win_rate) }}%</td>
                                    </tr>
                                {% endfor %}
                                </tbody>
                            </table>
                            {% endif %}
                        </div>
                    </div>
                    '''
                },
                'telegram': '''
📊 <b>Daily Trading Summary</b>
📅 <b>Date:</b> {{ date }}

💰 <b>Total P&L:</b> ${{ "%.2f"|format(total_pnl) }}
📈 <b>Total Trades:</b> {{ total_trades }}
🎯 <b>Win Rate:</b> {{ "%.1f"|format(win_rate) }}%
📊 <b>Active Positions:</b> {{ active_positions }}

{% if strategies %}
<b>Strategy Performance:</b>
{% for strategy in strategies %}
• {{ strategy.name }}: {{ strategy.trades }} trades, ${{ "%.2f"|format(strategy.pnl) }} P&L ({{ "%.1f"|format(strategy.win_rate) }}%)
{% endfor %}
{% endif %}
                '''
            }
        }

    def get_template(self, template_type: str, format_type: str) -> Template:
        """Get a template with caching and validation."""
        try:
            cache_key = f"{template_type}_{format_type}"

            # Check cache first
            if cache_key in self.template_cache:
                cached = self.template_cache[cache_key]
                if (datetime.now() - cached['timestamp']).seconds < self.cache_ttl:
                    return cached['template']

            # Get template configuration
            template_config = self.templates.get(template_type, {})
            if format_type not in template_config:
                raise TemplateError(f"Template not found: {template_type}.{format_type}")

            # Create template
            if format_type == 'email':
                template_content = template_config[format_type]['body']
            else:
                template_content = template_config[format_type]

            template = Template(template_content)

            # Validate template
            self._validate_template(template, template_type)

            # Cache template
            self.template_cache[cache_key] = {
                'template': template,
                'timestamp': datetime.now()
            }

            return template

        except Exception as e:
            log_exception_context(e, {'template_type': template_type, 'format_type': format_type})
            raise TemplateError(f"Template loading failed: {str(e)}")

    def get_template_subject(self, template_type: str) -> Template:
        """Get email subject template."""
        try:
            template_config = self.templates.get(template_type, {})
            if 'email' not in template_config or 'subject' not in template_config['email']:
                return Template(f"Notification: {template_type}")

            return Template(template_config['email']['subject'])

        except Exception as e:
            log_exception_context(e, {'template_type': template_type})
            return Template(f"Notification: {template_type}")

    def _validate_template(self, template: Template, template_type: str):
        """Validate template syntax and required variables."""
        try:
            # Test render with dummy data
            test_data = self._get_test_data(template_type)
            template.render(**test_data)

        except Exception as e:
            raise TemplateError(f"Template validation failed for {template_type}: {str(e)}")

    def _get_test_data(self, template_type: str) -> Dict[str, Any]:
        """Get test data for template validation."""
        test_data = {
            'trade_alert': {
                'alert_type': 'Trade Execution',
                'strategy_id': 'test_strategy',
                'symbol': 'BTC/USDT',
                'side': 'buy',
                'price': 50000.0,
                'quantity': 0.001,
                'timestamp': '2024-01-01 12:00:00',
                'message': 'Test message'
            },
            'risk_alert': {
                'alert_type': 'Position Size Limit',
                'severity': 'high',
                'message': 'Test risk alert',
                'timestamp': '2024-01-01 12:00:00',
                'details': {'limit': '10%', 'current': '12%'}
            },
            'daily_summary': {
                'date': '2024-01-01',
                'total_pnl': 125.50,
                'total_trades': 8,
                'win_rate': 62.5,
                'active_positions': 3,
                'strategies': [
                    {'name': 'Test Strategy', 'trades': 3, 'pnl': 45.20, 'win_rate': 66.7}
                ]
            }
        }

        return test_data.get(template_type, {})


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

        # Database connections for real data
        self.postgres_url = os.getenv('POSTGRES_URL', 'postgresql://admin:SecureDB2024!@postgresql:5432/trading')
        self.mongodb_url = os.getenv('MONGODB_URL', 'mongodb://admin:SecureDB2024!@mongodb:27017/trading_data?authSource=admin')
        self.db_pool = None
        self.mongo_client = None

        # Template management
        self.template_manager = DynamicTemplateManager()

        # Notification settings
        self.notification_settings = {
            'telegram_enabled': bool(self.telegram_token and self.telegram_chat_id),
            'email_enabled': bool(self.smtp_username and self.smtp_password and self.email_to),
            'trade_notifications': True,
            'risk_notifications': True,
            'system_notifications': True,
            'daily_summary': True,
            'rate_limit_seconds': 60,  # Minimum seconds between similar notifications
            'template_validation': True,
            'real_data_integration': True
        }

        # Control flags
        self.running = False

    async def connect_databases(self):
        """Connect to databases for real data access."""
        try:
            # Connect to PostgreSQL
            import asyncpg
            self.db_pool = await asyncpg.create_pool(self.postgres_url)

            # Connect to MongoDB
            import motor.motor_asyncio
            self.mongo_client = motor.motor_asyncio.AsyncIOMotorClient(self.mongodb_url)

            logger.info("Database connections established for notifications")

        except Exception as e:
            log_exception_context(e, {'component': 'database_connection'})
            raise DataAccessError(f"Failed to connect to databases: {str(e)}")

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

            # Connect to databases for real data integration
            if self.notification_settings['real_data_integration']:
                await self.connect_databases()

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
        """Send daily trading summary with real data integration."""
        try:
            if not self.notification_settings['daily_summary']:
                return

            today = datetime.now().date()

            # Get real daily statistics
            if self.notification_settings['real_data_integration']:
                daily_stats = await self._get_real_daily_stats(today)
            else:
                # Fallback to basic stats
                daily_stats = await self._get_basic_daily_stats(today)

            # Use dynamic templates
            try:
                # Telegram summary
                telegram_template = self.template_manager.get_template('daily_summary', 'telegram')
                telegram_message = telegram_template.render(**daily_stats)
                await self.send_telegram_message(telegram_message)

                # Email summary
                email_subject_template = self.template_manager.get_template_subject('daily_summary')
                email_subject = email_subject_template.render(**daily_stats)

                email_template = self.template_manager.get_template('daily_summary', 'email')
                email_content = email_template.render(**daily_stats)

                await self.send_email(email_subject, email_content)

            except TemplateError as e:
                logger.error(f"Template error in daily summary: {e}")
                # Fallback to simple message
                await self.send_telegram_message(f"📊 Daily Summary for {daily_stats['date']}: ${daily_stats['total_pnl']:.2f} P&L, {daily_stats['total_trades']} trades")
                await self.send_email(f"Daily Summary - {daily_stats['date']}", f"<h2>Daily Summary</h2><p>P&L: ${daily_stats['total_pnl']:.2f}</p>")

            logger.info("Daily summary sent")

        except Exception as e:
            log_exception_context(e, {'component': 'daily_summary'})
            raise NotificationError(f"Daily summary failed: {str(e)}")

    async def _get_real_daily_stats(self, date) -> Dict[str, Any]:
        """Get real daily statistics from database."""
        try:
            if not self.db_pool:
                await self.connect_databases()

            async with self.db_pool.acquire() as conn:
                # Get daily trade statistics
                trade_stats = await conn.fetchrow("""
                    SELECT
                        COUNT(*) as total_trades,
                        SUM(realized_pnl) as total_pnl,
                        COUNT(CASE WHEN realized_pnl > 0 THEN 1 END) as winning_trades,
                        COUNT(CASE WHEN status = 'open' THEN 1 END) as active_positions
                    FROM trades
                    WHERE DATE(created_at) = $1
                """, date)

                # Get strategy performance
                strategy_stats = await conn.fetch("""
                    SELECT
                        strategy_id as name,
                        COUNT(*) as trades,
                        SUM(realized_pnl) as pnl,
                        COUNT(CASE WHEN realized_pnl > 0 THEN 1 END) * 100.0 / COUNT(*) as win_rate
                    FROM trades
                    WHERE DATE(created_at) = $1
                    GROUP BY strategy_id
                    ORDER BY pnl DESC
                """, date)

                # Calculate win rate
                win_rate = 0.0
                if trade_stats['total_trades'] > 0:
                    win_rate = (trade_stats['winning_trades'] / trade_stats['total_trades']) * 100

                return {
                    'date': date.strftime('%Y-%m-%d'),
                    'total_pnl': float(trade_stats['total_pnl'] or 0),
                    'total_trades': trade_stats['total_trades'],
                    'win_rate': win_rate,
                    'active_positions': trade_stats['active_positions'],
                    'strategies': [
                        {
                            'name': row['name'],
                            'trades': row['trades'],
                            'pnl': float(row['pnl'] or 0),
                            'win_rate': float(row['win_rate'] or 0)
                        }
                        for row in strategy_stats
                    ]
                }

        except Exception as e:
            log_exception_context(e, {'date': date})
            logger.warning(f"Failed to get real daily stats, using fallback: {e}")
            return await self._get_basic_daily_stats(date)

    async def _get_basic_daily_stats(self, date) -> Dict[str, Any]:
        """Get basic daily statistics as fallback."""
        try:
            # Try to get some basic stats from Redis cache
            if self.redis_client:
                cache_key = f"daily_stats:{date.strftime('%Y-%m-%d')}"
                cached_stats = await self.redis_client.get(cache_key)
                if cached_stats:
                    import json
                    return json.loads(cached_stats)

            # Ultimate fallback - return minimal stats
            return {
                'date': date.strftime('%Y-%m-%d'),
                'total_pnl': 0.0,
                'total_trades': 0,
                'win_rate': 0.0,
                'active_positions': 0,
                'strategies': []
            }

        except Exception as e:
            log_exception_context(e, {'date': date})
            # Return minimal stats
            return {
                'date': date.strftime('%Y-%m-%d'),
                'total_pnl': 0.0,
                'total_trades': 0,
                'win_rate': 0.0,
                'active_positions': 0,
                'strategies': []
            }

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
