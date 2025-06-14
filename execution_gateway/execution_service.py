"""
Execution Gateway Service
Executes approved trading signals on exchanges.
"""
import asyncio
import logging
import os
import sys
import json
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List
import pymongo
from pymongo import MongoClient

# Add shared modules to path
sys.path.append('/app')
from shared.messaging import MessageConsumer, RabbitMQPublisher
from shared.exchange_factory import get_exchange
from shared.rate_limiter import ExchangeRateLimiters

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ExecutionGateway:
    """Execution gateway for placing and managing orders on exchanges."""
    
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://localhost/')
        self.mongodb_url = os.getenv('MONGODB_URL', 'mongodb://localhost:27017')
        
        # Messaging components
        self.signal_consumer = MessageConsumer(self.rabbitmq_url)
        self.rabbitmq_publisher = RabbitMQPublisher(self.rabbitmq_url)
        self.command_consumer = MessageConsumer(self.rabbitmq_url)
        
        # MongoDB for order tracking
        self.mongo_client = None
        self.db = None
        
        # Rate limiters
        self.rate_limiters = ExchangeRateLimiters('redis://localhost:6379')
        
        # Active orders tracking
        self.active_orders = {}  # order_id -> order_info
        
        # Control flags
        self.running = False
        self.execution_enabled = True
        
        # Default order parameters
        self.default_order_params = {
            'position_size_usd': 100.0,  # Default position size in USD
            'order_type': 'market',      # market, limit, stop_limit
            'time_in_force': 'GTC'       # GTC, IOC, FOK
        }
        
    async def initialize(self):
        """Initialize the execution gateway."""
        try:
            # Connect to messaging systems
            await self.signal_consumer.connect()
            await self.rabbitmq_publisher.connect()
            await self.command_consumer.connect()
            
            # Connect to MongoDB
            self.mongo_client = MongoClient(self.mongodb_url)
            self.db = self.mongo_client['trading_data']
            
            # Create indexes for orders collection
            self.db.orders.create_index("order_id", unique=True)
            self.db.orders.create_index("exchange_id")
            self.db.orders.create_index("symbol")
            self.db.orders.create_index("status")
            self.db.orders.create_index("timestamp")
            
            logger.info("Execution Gateway initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Execution Gateway: {e}")
            raise
    
    def calculate_order_quantity(self, signal: Dict[str, Any], current_price: float) -> float:
        """Calculate order quantity based on position sizing rules."""
        try:
            # Get position size from signal or use default
            position_size_usd = signal.get('position_size_usd', self.default_order_params['position_size_usd'])
            
            # Calculate quantity
            quantity = position_size_usd / current_price
            
            # Round to appropriate precision (simplified)
            if quantity < 0.001:
                quantity = round(quantity, 6)
            elif quantity < 0.1:
                quantity = round(quantity, 4)
            else:
                quantity = round(quantity, 2)
            
            return quantity
            
        except Exception as e:
            logger.error(f"Error calculating order quantity: {e}")
            return 0.0
    
    async def place_market_order(self, exchange, signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Place a market order on the exchange."""
        try:
            symbol = signal['symbol']
            side = signal['side']
            current_price = signal['price']
            
            # Calculate quantity
            quantity = self.calculate_order_quantity(signal, current_price)
            
            if quantity <= 0:
                logger.error(f"Invalid quantity {quantity} for {symbol}")
                return None
            
            # Place market order
            order = await exchange.create_market_order(symbol, side, quantity)
            
            logger.info(f"Placed market order: {order['id']} {side} {quantity} {symbol}")
            return order
            
        except Exception as e:
            logger.error(f"Error placing market order: {e}")
            return None
    
    async def place_limit_order(self, exchange, signal: Dict[str, Any], limit_price: float) -> Optional[Dict[str, Any]]:
        """Place a limit order on the exchange."""
        try:
            symbol = signal['symbol']
            side = signal['side']
            
            # Calculate quantity
            quantity = self.calculate_order_quantity(signal, limit_price)
            
            if quantity <= 0:
                logger.error(f"Invalid quantity {quantity} for {symbol}")
                return None
            
            # Place limit order
            order = await exchange.create_limit_order(symbol, side, quantity, limit_price)
            
            logger.info(f"Placed limit order: {order['id']} {side} {quantity} {symbol} @ {limit_price}")
            return order
            
        except Exception as e:
            logger.error(f"Error placing limit order: {e}")
            return None
    
    async def place_oco_order(self, exchange, signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Place an OCO (One-Cancels-Other) order with stop loss and take profit."""
        try:
            symbol = signal['symbol']
            side = signal['side']
            current_price = signal['price']
            
            # Get stop loss and take profit from signal
            stop_loss = signal.get('stop_loss')
            take_profit = signal.get('take_profit')
            
            if not stop_loss or not take_profit:
                logger.warning(f"Missing stop loss or take profit for OCO order: {signal}")
                return await self.place_market_order(exchange, signal)
            
            # Calculate quantity
            quantity = self.calculate_order_quantity(signal, current_price)
            
            if quantity <= 0:
                logger.error(f"Invalid quantity {quantity} for {symbol}")
                return None
            
            # First place the entry order (market order)
            entry_order = await exchange.create_market_order(symbol, side, quantity)
            
            if not entry_order:
                return None
            
            # Then place OCO order for exit
            exit_side = 'sell' if side == 'buy' else 'buy'
            
            # Check if exchange supports OCO orders
            if exchange.has.get('createOrder') and 'oco' in str(exchange.describe()):
                try:
                    oco_order = await exchange.create_order(
                        symbol, 'oco', exit_side, quantity, None, {
                            'stopPrice': stop_loss,
                            'price': take_profit,
                            'stopLimitPrice': stop_loss * 0.99 if side == 'buy' else stop_loss * 1.01
                        }
                    )
                    logger.info(f"Placed OCO order: {oco_order['id']}")
                    return {'entry': entry_order, 'oco': oco_order}
                except Exception as e:
                    logger.warning(f"OCO order failed, placing separate orders: {e}")
            
            # Fallback: place separate stop loss and take profit orders
            try:
                stop_order = await exchange.create_stop_limit_order(
                    symbol, exit_side, quantity, stop_loss, stop_loss * 0.99 if side == 'buy' else stop_loss * 1.01
                )
                
                tp_order = await exchange.create_limit_order(
                    symbol, exit_side, quantity, take_profit
                )
                
                return {
                    'entry': entry_order,
                    'stop_loss': stop_order,
                    'take_profit': tp_order
                }
                
            except Exception as e:
                logger.error(f"Failed to place exit orders: {e}")
                return {'entry': entry_order}
            
        except Exception as e:
            logger.error(f"Error placing OCO order: {e}")
            return None
    
    async def execute_signal(self, signal: Dict[str, Any]):
        """Execute an approved trading signal."""
        try:
            if not self.execution_enabled:
                logger.info("Execution disabled, skipping signal")
                return
            
            exchange_id = signal['exchange']
            symbol = signal['symbol']
            
            logger.info(f"Executing signal: {signal['strategy_id']} {signal['side']} {symbol}")
            
            # Get exchange instance
            exchange = await get_exchange(exchange_id, use_testnet=True)
            
            # Apply rate limiting
            rate_limiter = self.rate_limiters.get_limiter(exchange_id)
            await rate_limiter.acquire()
            
            # Determine order type
            order_type = signal.get('order_type', self.default_order_params['order_type'])
            
            # Execute based on order type
            order_result = None
            
            if order_type == 'market':
                order_result = await self.place_market_order(exchange, signal)
            elif order_type == 'limit':
                limit_price = signal.get('limit_price', signal['price'])
                order_result = await self.place_limit_order(exchange, signal, limit_price)
            elif order_type == 'oco':
                order_result = await self.place_oco_order(exchange, signal)
            else:
                logger.error(f"Unknown order type: {order_type}")
                return
            
            if order_result:
                # Store order information
                await self.store_order_info(signal, order_result)
                
                # Publish fill event
                await self.publish_fill_event(signal, order_result)
                
                # Track active orders
                if isinstance(order_result, dict) and 'id' in order_result:
                    self.active_orders[order_result['id']] = {
                        'signal': signal,
                        'order': order_result,
                        'timestamp': datetime.now()
                    }
            
        except Exception as e:
            logger.error(f"Error executing signal: {e}")
            
            # Publish error alert
            alert_message = {
                'type': 'execution_error',
                'signal': signal,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            await self.rabbitmq_publisher.publish(alert_message, 'trading.alerts')
    
    async def store_order_info(self, signal: Dict[str, Any], order_result: Dict[str, Any]):
        """Store order information in MongoDB."""
        try:
            # Handle different order result formats
            orders_to_store = []
            
            if isinstance(order_result, dict):
                if 'entry' in order_result:
                    # OCO or multi-order result
                    for order_type, order in order_result.items():
                        if order and isinstance(order, dict):
                            orders_to_store.append((order_type, order))
                else:
                    # Single order result
                    orders_to_store.append(('main', order_result))
            
            for order_type, order in orders_to_store:
                order_doc = {
                    'order_id': order.get('id', str(uuid.uuid4())),
                    'exchange_id': signal['exchange'],
                    'symbol': signal['symbol'],
                    'strategy_id': signal['strategy_id'],
                    'side': signal['side'],
                    'order_type': order_type,
                    'quantity': order.get('amount', 0),
                    'price': order.get('price', signal['price']),
                    'status': order.get('status', 'unknown'),
                    'timestamp': datetime.now(),
                    'signal_data': signal,
                    'exchange_response': order
                }
                
                self.db.orders.insert_one(order_doc)
                logger.debug(f"Stored order info: {order_doc['order_id']}")
            
        except Exception as e:
            logger.error(f"Error storing order info: {e}")
    
    async def publish_fill_event(self, signal: Dict[str, Any], order_result: Dict[str, Any]):
        """Publish fill event for portfolio accounting."""
        try:
            # Create fill event message
            fill_event = {
                'type': 'fill',
                'exchange': signal['exchange'],
                'symbol': signal['symbol'],
                'strategy_id': signal['strategy_id'],
                'side': signal['side'],
                'quantity': 0,
                'price': signal['price'],
                'timestamp': int(datetime.now().timestamp() * 1000),
                'order_id': '',
                'commission': 0.0,
                'signal_metadata': signal.get('metadata', {})
            }
            
            # Extract order details
            if isinstance(order_result, dict):
                if 'entry' in order_result:
                    # Use entry order for fill event
                    entry_order = order_result['entry']
                    fill_event['order_id'] = entry_order.get('id', '')
                    fill_event['quantity'] = entry_order.get('amount', 0)
                    fill_event['price'] = entry_order.get('price', signal['price'])
                else:
                    # Single order
                    fill_event['order_id'] = order_result.get('id', '')
                    fill_event['quantity'] = order_result.get('amount', 0)
                    fill_event['price'] = order_result.get('price', signal['price'])
            
            # Publish to fills queue
            await self.rabbitmq_publisher.publish(fill_event, 'trading.fills')
            logger.info(f"Published fill event: {fill_event['order_id']}")
            
        except Exception as e:
            logger.error(f"Error publishing fill event: {e}")
    
    async def monitor_orders(self):
        """Monitor active orders for status updates."""
        try:
            if not self.active_orders:
                return
            
            # Check order status periodically
            orders_to_remove = []
            
            for order_id, order_info in self.active_orders.items():
                try:
                    signal = order_info['signal']
                    exchange = await get_exchange(signal['exchange'], use_testnet=True)
                    
                    # Fetch order status
                    order_status = await exchange.fetch_order(order_id, signal['symbol'])
                    
                    if order_status['status'] in ['closed', 'canceled', 'expired']:
                        # Order is complete, remove from tracking
                        orders_to_remove.append(order_id)
                        
                        # Update order in database
                        self.db.orders.update_one(
                            {'order_id': order_id},
                            {'$set': {
                                'status': order_status['status'],
                                'filled_quantity': order_status.get('filled', 0),
                                'remaining_quantity': order_status.get('remaining', 0),
                                'updated_at': datetime.now()
                            }}
                        )
                        
                        logger.info(f"Order {order_id} completed with status: {order_status['status']}")
                
                except Exception as e:
                    logger.error(f"Error monitoring order {order_id}: {e}")
            
            # Remove completed orders
            for order_id in orders_to_remove:
                del self.active_orders[order_id]
                
        except Exception as e:
            logger.error(f"Error in order monitoring: {e}")
    
    async def handle_command(self, command: Dict[str, Any]):
        """Handle execution gateway commands."""
        try:
            command_type = command.get('type')
            
            if command_type == 'enable_execution':
                self.execution_enabled = True
                logger.info("Execution enabled")
                
            elif command_type == 'disable_execution':
                self.execution_enabled = False
                logger.warning("Execution disabled")
                
            elif command_type == 'cancel_order':
                order_id = command.get('order_id')
                exchange_id = command.get('exchange_id')
                symbol = command.get('symbol')
                
                if order_id and exchange_id and symbol:
                    await self.cancel_order(order_id, exchange_id, symbol)
                    
            elif command_type == 'get_execution_status':
                status = {
                    'execution_enabled': self.execution_enabled,
                    'active_orders_count': len(self.active_orders),
                    'active_orders': list(self.active_orders.keys())
                }
                await self.rabbitmq_publisher.publish(status, 'execution.status')
                
            else:
                logger.warning(f"Unknown command type: {command_type}")
                
        except Exception as e:
            logger.error(f"Error handling command: {e}")
    
    async def cancel_order(self, order_id: str, exchange_id: str, symbol: str):
        """Cancel an active order."""
        try:
            exchange = await get_exchange(exchange_id, use_testnet=True)
            
            # Cancel order on exchange
            result = await exchange.cancel_order(order_id, symbol)
            
            # Update database
            self.db.orders.update_one(
                {'order_id': order_id},
                {'$set': {
                    'status': 'canceled',
                    'updated_at': datetime.now()
                }}
            )
            
            # Remove from active orders
            if order_id in self.active_orders:
                del self.active_orders[order_id]
            
            logger.info(f"Canceled order: {order_id}")
            
        except Exception as e:
            logger.error(f"Error canceling order {order_id}: {e}")
    
    async def run(self):
        """Main service loop."""
        self.running = True
        
        try:
            await self.initialize()
            
            # Start consuming approved signals
            signal_task = asyncio.create_task(
                self.signal_consumer.consume(
                    'approved-signals',
                    self.execute_signal,
                    'trading.approved-signals'
                )
            )
            
            # Start consuming commands
            command_task = asyncio.create_task(
                self.command_consumer.consume(
                    'execution-commands',
                    self.handle_command,
                    'commands.execution'
                )
            )
            
            # Start order monitoring
            async def monitor_loop():
                while self.running:
                    await self.monitor_orders()
                    await asyncio.sleep(30)  # Check every 30 seconds
            
            monitor_task = asyncio.create_task(monitor_loop())
            
            logger.info("Execution Gateway started successfully")
            
            # Run all tasks
            await asyncio.gather(signal_task, command_task, monitor_task, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error in main service loop: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources."""
        self.running = False
        
        try:
            await self.signal_consumer.close()
            await self.rabbitmq_publisher.close()
            await self.command_consumer.close()
            await self.rate_limiters.close_all()
            
            if self.mongo_client:
                self.mongo_client.close()
                
            logger.info("Execution Gateway cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    """Main entry point."""
    gateway = ExecutionGateway()
    
    try:
        await gateway.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Service error: {e}")
    finally:
        await gateway.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
