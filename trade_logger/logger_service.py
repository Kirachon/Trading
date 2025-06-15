"""
Trade Logger Service
Comprehensive logging and audit trail for all trading activities.
"""
import asyncio
import logging
import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from motor.motor_asyncio import AsyncIOMotorClient
import pandas as pd

# Add shared modules to path
sys.path.append('/app')
from shared.messaging import MessageConsumer, RabbitMQPublisher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TradeLoggerService:
    """Trade logger service for comprehensive audit trail and analytics."""
    
    def __init__(self):
        self.mongodb_url = os.getenv('MONGODB_URL', 'mongodb://localhost:27017')
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://localhost/')
        
        # Messaging components
        self.fill_consumer = MessageConsumer(self.rabbitmq_url)
        self.signal_consumer = MessageConsumer(self.rabbitmq_url)
        self.alert_consumer = MessageConsumer(self.rabbitmq_url)
        self.portfolio_consumer = MessageConsumer(self.rabbitmq_url)
        self.rabbitmq_publisher = RabbitMQPublisher(self.rabbitmq_url)
        self.command_consumer = MessageConsumer(self.rabbitmq_url)
        
        # MongoDB client
        self.mongo_client = None
        self.db = None
        
        # Collections
        self.collections = {
            'fills': None,
            'signals': None,
            'alerts': None,
            'portfolio_updates': None,
            'system_events': None,
            'performance_logs': None
        }
        
        # Control flags
        self.running = False
        
    async def initialize(self):
        """Initialize the trade logger service."""
        try:
            # Connect to messaging systems
            await self.fill_consumer.connect()
            await self.signal_consumer.connect()
            await self.alert_consumer.connect()
            await self.portfolio_consumer.connect()
            await self.rabbitmq_publisher.connect()
            await self.command_consumer.connect()
            
            # Connect to MongoDB using Motor (async)
            self.mongo_client = AsyncIOMotorClient(self.mongodb_url)
            self.db = self.mongo_client['trading_logs']

            # Test the connection
            await self.mongo_client.admin.command('ping')

            # Initialize collections
            await self.setup_collections()
            
            logger.info("Trade Logger Service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Trade Logger Service: {e}")
            raise
    
    async def setup_collections(self):
        """Set up MongoDB collections with proper indexes."""
        try:
            # Initialize collections
            for collection_name in self.collections.keys():
                self.collections[collection_name] = self.db[collection_name]

            # Create indexes for efficient querying (async)

            # Fills collection indexes
            await self.collections['fills'].create_index([("timestamp", 1)])
            await self.collections['fills'].create_index([("exchange", 1), ("symbol", 1)])
            await self.collections['fills'].create_index([("strategy_id", 1)])
            await self.collections['fills'].create_index([("order_id", 1)])

            # Signals collection indexes
            await self.collections['signals'].create_index([("timestamp", 1)])
            await self.collections['signals'].create_index([("strategy_id", 1)])
            await self.collections['signals'].create_index([("symbol", 1)])
            await self.collections['signals'].create_index([("side", 1)])

            # Alerts collection indexes
            await self.collections['alerts'].create_index([("timestamp", 1)])
            await self.collections['alerts'].create_index([("type", 1)])
            await self.collections['alerts'].create_index([("severity", 1)])

            # Portfolio updates indexes
            await self.collections['portfolio_updates'].create_index([("timestamp", 1)])
            await self.collections['portfolio_updates'].create_index([("trade_id", 1)])

            # System events indexes
            await self.collections['system_events'].create_index([("timestamp", 1)])
            await self.collections['system_events'].create_index([("service", 1)])
            await self.collections['system_events'].create_index([("event_type", 1)])

            # Performance logs indexes
            await self.collections['performance_logs'].create_index([("timestamp", 1)])
            await self.collections['performance_logs'].create_index([("strategy_id", 1)])
            await self.collections['performance_logs'].create_index([("date", 1)])

            logger.info("MongoDB collections and indexes set up successfully")

        except Exception as e:
            logger.error(f"Error setting up collections: {e}")
            raise
    
    async def log_fill_event(self, fill_event: Dict[str, Any]):
        """Log fill event to MongoDB."""
        try:
            # Enhance fill event with additional metadata
            enhanced_fill = {
                **fill_event,
                'logged_at': datetime.now(),
                'log_type': 'fill',
                'trade_value': float(fill_event.get('quantity', 0)) * float(fill_event.get('price', 0)),
                'commission_usd': float(fill_event.get('commission', 0)),
                'market_conditions': await self.get_market_conditions(fill_event)
            }
            
            # Insert into fills collection
            result = await self.collections['fills'].insert_one(enhanced_fill)

            logger.info(f"Logged fill event: {fill_event.get('order_id', 'unknown')} - {result.inserted_id}")
            
            # Generate performance analytics
            await self.update_performance_analytics(fill_event)
            
        except Exception as e:
            logger.error(f"Error logging fill event: {e}")
    
    async def log_signal_event(self, signal: Dict[str, Any]):
        """Log trading signal to MongoDB."""
        try:
            # Enhance signal with metadata
            enhanced_signal = {
                **signal,
                'logged_at': datetime.now(),
                'log_type': 'signal',
                'signal_strength': abs(float(signal.get('signal', 0))),
                'confidence_level': signal.get('confidence', 0),
                'market_context': await self.get_market_context(signal)
            }
            
            # Insert into signals collection
            result = await self.collections['signals'].insert_one(enhanced_signal)

            logger.debug(f"Logged signal: {signal.get('strategy_id', 'unknown')} {signal.get('side', 'unknown')} - {result.inserted_id}")
            
        except Exception as e:
            logger.error(f"Error logging signal: {e}")
    
    async def log_alert_event(self, alert: Dict[str, Any]):
        """Log alert/notification to MongoDB."""
        try:
            # Enhance alert with metadata
            enhanced_alert = {
                **alert,
                'logged_at': datetime.now(),
                'log_type': 'alert',
                'severity': self.determine_alert_severity(alert),
                'resolved': False,
                'resolution_time': None
            }
            
            # Insert into alerts collection
            result = await self.collections['alerts'].insert_one(enhanced_alert)

            logger.info(f"Logged alert: {alert.get('type', 'unknown')} - {result.inserted_id}")
            
        except Exception as e:
            logger.error(f"Error logging alert: {e}")
    
    async def log_portfolio_update(self, update: Dict[str, Any]):
        """Log portfolio update to MongoDB."""
        try:
            # Enhance portfolio update
            enhanced_update = {
                **update,
                'logged_at': datetime.now(),
                'log_type': 'portfolio_update'
            }
            
            # Insert into portfolio updates collection
            result = await self.collections['portfolio_updates'].insert_one(enhanced_update)

            logger.debug(f"Logged portfolio update: {update.get('trade_id', 'unknown')} - {result.inserted_id}")
            
        except Exception as e:
            logger.error(f"Error logging portfolio update: {e}")
    
    async def log_system_event(self, event: Dict[str, Any]):
        """Log system event (service start/stop, errors, etc.)."""
        try:
            # Enhance system event
            enhanced_event = {
                **event,
                'logged_at': datetime.now(),
                'log_type': 'system_event',
                'hostname': os.getenv('HOSTNAME', 'unknown'),
                'service_version': '1.0.0'
            }
            
            # Insert into system events collection
            result = await self.collections['system_events'].insert_one(enhanced_event)

            logger.info(f"Logged system event: {event.get('event_type', 'unknown')} - {result.inserted_id}")
            
        except Exception as e:
            logger.error(f"Error logging system event: {e}")
    
    async def get_market_conditions(self, fill_event: Dict[str, Any]) -> Dict[str, Any]:
        """Get market conditions at the time of fill."""
        try:
            # Simplified market conditions - in production would fetch real market data
            return {
                'volatility_regime': 'normal',  # Would calculate from recent price data
                'market_hours': 'active',       # Crypto markets are always active
                'liquidity_estimate': 'high',   # Would estimate from order book data
                'spread_estimate': 0.001        # Would calculate from bid/ask spread
            }
        except Exception as e:
            logger.error(f"Error getting market conditions: {e}")
            return {}
    
    async def get_market_context(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """Get market context for signal generation."""
        try:
            return {
                'market_trend': 'neutral',      # Would analyze from price data
                'volume_profile': 'average',    # Would analyze from volume data
                'correlation_regime': 'normal', # Would analyze cross-asset correlations
                'news_sentiment': 'neutral'     # Would integrate news sentiment analysis
            }
        except Exception as e:
            logger.error(f"Error getting market context: {e}")
            return {}
    
    def determine_alert_severity(self, alert: Dict[str, Any]) -> str:
        """Determine alert severity level."""
        try:
            alert_type = alert.get('type', '').lower()
            
            if 'error' in alert_type or 'failure' in alert_type:
                return 'high'
            elif 'risk' in alert_type or 'breach' in alert_type:
                return 'medium'
            elif 'warning' in alert_type:
                return 'low'
            else:
                return 'info'
                
        except Exception as e:
            logger.error(f"Error determining alert severity: {e}")
            return 'unknown'
    
    async def update_performance_analytics(self, fill_event: Dict[str, Any]):
        """Update performance analytics based on fill event."""
        try:
            strategy_id = fill_event.get('strategy_id')
            if not strategy_id:
                return
            
            # Calculate daily performance metrics
            today = datetime.now().date()
            
            # Get today's fills for this strategy
            today_start = datetime.combine(today, datetime.min.time())
            today_end = datetime.combine(today, datetime.max.time())
            
            fills_today = await self.collections['fills'].find({
                'strategy_id': strategy_id,
                'logged_at': {'$gte': today_start, '$lte': today_end}
            }).to_list(length=None)
            
            if fills_today:
                # Calculate metrics
                total_trades = len(fills_today)
                total_volume = sum(fill.get('trade_value', 0) for fill in fills_today)
                total_commission = sum(fill.get('commission_usd', 0) for fill in fills_today)
                
                # Calculate win/loss (simplified)
                profitable_trades = sum(1 for fill in fills_today if fill.get('side') == 'sell')  # Simplified
                win_rate = profitable_trades / total_trades if total_trades > 0 else 0
                
                # Update or insert performance log
                performance_log = {
                    'strategy_id': strategy_id,
                    'date': today,
                    'total_trades': total_trades,
                    'total_volume': total_volume,
                    'total_commission': total_commission,
                    'win_rate': win_rate,
                    'last_updated': datetime.now(),
                    'log_type': 'performance'
                }
                
                # Upsert performance log
                await self.collections['performance_logs'].replace_one(
                    {'strategy_id': strategy_id, 'date': today},
                    performance_log,
                    upsert=True
                )
                
                logger.debug(f"Updated performance analytics for {strategy_id}")
                
        except Exception as e:
            logger.error(f"Error updating performance analytics: {e}")
    
    async def get_trade_history(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get trade history with filters."""
        try:
            query = {}
            
            # Apply filters
            if 'strategy_id' in filters:
                query['strategy_id'] = filters['strategy_id']
            
            if 'symbol' in filters:
                query['symbol'] = filters['symbol']
            
            if 'start_date' in filters:
                query['logged_at'] = {'$gte': filters['start_date']}
            
            if 'end_date' in filters:
                if 'logged_at' not in query:
                    query['logged_at'] = {}
                query['logged_at']['$lte'] = filters['end_date']
            
            # Get fills with query
            fills = await self.collections['fills'].find(query).sort('logged_at', -1).limit(1000).to_list(length=1000)
            
            # Convert ObjectId to string for JSON serialization
            for fill in fills:
                fill['_id'] = str(fill['_id'])
                if 'logged_at' in fill:
                    fill['logged_at'] = fill['logged_at'].isoformat()
            
            return fills
            
        except Exception as e:
            logger.error(f"Error getting trade history: {e}")
            return []
    
    async def get_performance_summary(self, strategy_id: str, days: int = 30) -> Dict[str, Any]:
        """Get performance summary for a strategy."""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Get performance logs for the period
            performance_logs = list(self.collections['performance_logs'].find({
                'strategy_id': strategy_id,
                'date': {'$gte': start_date.date(), '$lte': end_date.date()}
            }).sort('date', 1))
            
            if not performance_logs:
                return {'strategy_id': strategy_id, 'summary': 'No data available'}
            
            # Calculate summary metrics
            total_trades = sum(log.get('total_trades', 0) for log in performance_logs)
            total_volume = sum(log.get('total_volume', 0) for log in performance_logs)
            total_commission = sum(log.get('total_commission', 0) for log in performance_logs)
            avg_win_rate = sum(log.get('win_rate', 0) for log in performance_logs) / len(performance_logs)
            
            # Trading days
            trading_days = len(performance_logs)
            avg_trades_per_day = total_trades / trading_days if trading_days > 0 else 0
            
            return {
                'strategy_id': strategy_id,
                'period_days': days,
                'trading_days': trading_days,
                'total_trades': total_trades,
                'avg_trades_per_day': round(avg_trades_per_day, 2),
                'total_volume': round(total_volume, 2),
                'total_commission': round(total_commission, 2),
                'avg_win_rate': round(avg_win_rate, 4),
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting performance summary: {e}")
            return {'error': str(e)}
    
    async def handle_command(self, command: Dict[str, Any]):
        """Handle trade logger commands."""
        try:
            command_type = command.get('type')
            
            if command_type == 'get_trade_history':
                filters = command.get('filters', {})
                history = await self.get_trade_history(filters)
                await self.rabbitmq_publisher.publish({'trade_history': history}, 'logger.trade_history')
                
            elif command_type == 'get_performance_summary':
                strategy_id = command.get('strategy_id')
                days = command.get('days', 30)
                if strategy_id:
                    summary = await self.get_performance_summary(strategy_id, days)
                    await self.rabbitmq_publisher.publish(summary, 'logger.performance_summary')
                    
            elif command_type == 'cleanup_old_logs':
                days_to_keep = command.get('days_to_keep', 90)
                await self.cleanup_old_logs(days_to_keep)
                
            else:
                logger.warning(f"Unknown command type: {command_type}")
                
        except Exception as e:
            logger.error(f"Error handling command: {e}")
    
    async def cleanup_old_logs(self, days_to_keep: int):
        """Clean up old log entries."""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Clean up old logs from each collection
            for collection_name, collection in self.collections.items():
                if collection_name != 'performance_logs':  # Keep performance logs longer
                    result = await collection.delete_many({'logged_at': {'$lt': cutoff_date}})
                    logger.info(f"Cleaned up {result.deleted_count} old records from {collection_name}")

            # Clean up very old performance logs (keep 1 year)
            old_cutoff = datetime.now() - timedelta(days=365)
            result = await self.collections['performance_logs'].delete_many({'last_updated': {'$lt': old_cutoff}})
            logger.info(f"Cleaned up {result.deleted_count} old performance logs")
            
        except Exception as e:
            logger.error(f"Error cleaning up old logs: {e}")
    
    async def run(self):
        """Main service loop."""
        self.running = True
        
        try:
            await self.initialize()
            
            # Start consuming different event types
            fill_task = asyncio.create_task(
                self.fill_consumer.consume('fills-log', self.log_fill_event, 'trading.fills')
            )
            
            signal_task = asyncio.create_task(
                self.signal_consumer.consume('signals-log', self.log_signal_event, 'trading.signals')
            )
            
            alert_task = asyncio.create_task(
                self.alert_consumer.consume('alerts-log', self.log_alert_event, 'trading.alerts')
            )
            
            portfolio_task = asyncio.create_task(
                self.portfolio_consumer.consume('portfolio-log', self.log_portfolio_update, 'portfolio.updates')
            )
            
            command_task = asyncio.create_task(
                self.command_consumer.consume('logger-commands', self.handle_command, 'commands.logger')
            )
            
            logger.info("Trade Logger Service started successfully")
            
            # Run all tasks
            await asyncio.gather(
                fill_task, signal_task, alert_task, portfolio_task, command_task,
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
            await self.fill_consumer.close()
            await self.signal_consumer.close()
            await self.alert_consumer.close()
            await self.portfolio_consumer.close()
            await self.rabbitmq_publisher.close()
            await self.command_consumer.close()
            
            if self.mongo_client:
                self.mongo_client.close()
                
            logger.info("Trade Logger Service cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    """Main entry point."""
    service = TradeLoggerService()
    
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
