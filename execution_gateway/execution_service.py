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
import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

# Add shared modules to path
sys.path.append('/app')
from shared.messaging import MessageConsumer, RabbitMQPublisher
from shared.exchange_factory import get_exchange
from shared.rate_limiter import ExchangeRateLimiters
from shared.exceptions import (
    TradingSystemError, ExecutionError, OrderRejectedError, OrderTimeoutError,
    InsufficientLiquidityError, RecoveryStrategy, log_exception_context
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AtomicOrderGroup:
    """Manages a group of related orders that should be executed atomically."""

    def __init__(self, group_id: str, strategy_id: str, symbol: str):
        self.group_id = group_id
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.orders = {}  # order_type -> order_info
        self.state = 'pending'  # pending, executing, completed, failed, cancelled
        self.created_at = datetime.now()
        self.updated_at = datetime.now()

    def add_order(self, order_type: str, order_info: Dict[str, Any]):
        """Add an order to the group."""
        self.orders[order_type] = order_info
        self.updated_at = datetime.now()

    def get_order(self, order_type: str) -> Optional[Dict[str, Any]]:
        """Get an order from the group."""
        return self.orders.get(order_type)

    def update_state(self, new_state: str):
        """Update the group state."""
        self.state = new_state
        self.updated_at = datetime.now()

    def is_complete(self) -> bool:
        """Check if all orders in the group are complete."""
        return self.state in ['completed', 'failed', 'cancelled']

    def get_active_orders(self) -> List[Dict[str, Any]]:
        """Get all active orders in the group."""
        return [order for order in self.orders.values() if order.get('status') not in ['closed', 'canceled', 'expired']]


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
        self.order_groups = {}   # group_id -> list of related orders (for atomic operations)

        # Order state management
        self.order_states = {}   # order_id -> state_info

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
            
            # Connect to MongoDB using Motor (async)
            self.mongo_client = AsyncIOMotorClient(self.mongodb_url)
            self.db = self.mongo_client['trading_data']

            # Test the connection
            await self.mongo_client.admin.command('ping')

            # Create indexes for orders collection (async)
            await self.db.orders.create_index("order_id", unique=True)
            await self.db.orders.create_index("exchange_id")
            await self.db.orders.create_index("symbol")
            await self.db.orders.create_index("status")
            await self.db.orders.create_index("timestamp")
            
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
    
    async def place_atomic_order_group(self, exchange, signal: Dict[str, Any]) -> Optional[AtomicOrderGroup]:
        """Place an atomic order group with proper exit order management."""
        group_id = str(uuid.uuid4())
        order_group = AtomicOrderGroup(group_id, signal['strategy_id'], signal['symbol'])

        try:
            symbol = signal['symbol']
            side = signal['side']
            current_price = signal['price']

            # Get stop loss and take profit from signal
            stop_loss = signal.get('stop_loss')
            take_profit = signal.get('take_profit')

            if not stop_loss or not take_profit:
                logger.warning(f"Missing stop loss or take profit for atomic order group: {signal}")
                # Fall back to simple market order
                entry_order = await self.place_market_order(exchange, signal)
                if entry_order:
                    order_group.add_order('entry', entry_order)
                    order_group.update_state('completed')
                return order_group

            # Calculate quantity
            quantity = self.calculate_order_quantity(signal, current_price)

            if quantity <= 0:
                raise ExecutionError(f"Invalid quantity {quantity} for {symbol}")

            order_group.update_state('executing')

            # Step 1: Place the entry order (market order)
            try:
                entry_order = await exchange.create_market_order(symbol, side, quantity)
                if not entry_order:
                    raise ExecutionError("Failed to place entry order")

                order_group.add_order('entry', entry_order)
                logger.info(f"Placed entry order: {entry_order['id']} for group {group_id}")

            except Exception as e:
                order_group.update_state('failed')
                log_exception_context(e, {'group_id': group_id, 'step': 'entry_order'})
                raise ExecutionError(f"Entry order failed: {str(e)}")

            # Step 2: Place exit orders atomically
            exit_side = 'sell' if side == 'buy' else 'buy'

            # Try native OCO first if supported
            if exchange.has.get('createOrder') and 'oco' in str(exchange.describe()):
                try:
                    oco_order = await exchange.create_order(
                        symbol, 'oco', exit_side, quantity, None, {
                            'stopPrice': stop_loss,
                            'price': take_profit,
                            'stopLimitPrice': stop_loss * 0.99 if side == 'buy' else stop_loss * 1.01
                        }
                    )
                    order_group.add_order('oco', oco_order)
                    order_group.update_state('completed')
                    logger.info(f"Placed native OCO order: {oco_order['id']} for group {group_id}")
                    return order_group

                except Exception as e:
                    logger.warning(f"Native OCO failed for group {group_id}, using managed exit orders: {e}")

            # Step 3: Use managed exit orders with proper cancellation logic
            try:
                # Place both exit orders
                stop_order = await exchange.create_stop_limit_order(
                    symbol, exit_side, quantity, stop_loss,
                    stop_loss * 0.99 if side == 'buy' else stop_loss * 1.01
                )

                tp_order = await exchange.create_limit_order(
                    symbol, exit_side, quantity, take_profit
                )

                if not stop_order or not tp_order:
                    # If either order failed, cancel the successful one
                    if stop_order:
                        await exchange.cancel_order(stop_order['id'], symbol)
                    if tp_order:
                        await exchange.cancel_order(tp_order['id'], symbol)
                    raise ExecutionError("Failed to place both exit orders atomically")

                order_group.add_order('stop_loss', stop_order)
                order_group.add_order('take_profit', tp_order)
                order_group.update_state('completed')

                logger.info(f"Placed managed exit orders for group {group_id}: SL={stop_order['id']}, TP={tp_order['id']}")
                return order_group

            except Exception as e:
                order_group.update_state('failed')
                log_exception_context(e, {'group_id': group_id, 'step': 'exit_orders'})
                raise ExecutionError(f"Exit orders failed: {str(e)}")

        except ExecutionError:
            # Re-raise execution errors
            raise
        except Exception as e:
            order_group.update_state('failed')
            log_exception_context(e, {'group_id': group_id})
            raise ExecutionError(f"Atomic order group failed: {str(e)}")

    async def place_oco_order(self, exchange, signal: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Legacy OCO method - now uses atomic order groups."""
        try:
            order_group = await self.place_atomic_order_group(exchange, signal)
            if order_group and order_group.state == 'completed':
                # Convert to legacy format for compatibility
                result = {}
                for order_type, order in order_group.orders.items():
                    result[order_type] = order
                return result
            return None

        except Exception as e:
            log_exception_context(e, {'signal': signal})
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
                
                # Track active orders and order groups
                if isinstance(order_result, AtomicOrderGroup):
                    # Track the order group
                    self.order_groups[order_result.group_id] = order_result

                    # Track individual orders within the group
                    for order_type, order in order_result.orders.items():
                        if order and 'id' in order:
                            self.active_orders[order['id']] = {
                                'signal': signal,
                                'order': order,
                                'order_type': order_type,
                                'group_id': order_result.group_id,
                                'timestamp': datetime.now()
                            }
                elif isinstance(order_result, dict) and 'id' in order_result:
                    self.active_orders[order_result['id']] = {
                        'signal': signal,
                        'order': order_result,
                        'order_type': 'single',
                        'group_id': None,
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
                
                await self.db.orders.insert_one(order_doc)
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
        """Monitor active orders and order groups for status updates."""
        try:
            if not self.active_orders and not self.order_groups:
                return

            # Monitor individual orders
            await self._monitor_individual_orders()

            # Monitor order groups for exit order management
            await self._monitor_order_groups()

        except Exception as e:
            log_exception_context(e, {'component': 'order_monitoring'})

    async def _monitor_individual_orders(self):
        """Monitor individual order status updates."""
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
                    await self.db.orders.update_one(
                        {'order_id': order_id},
                        {'$set': {
                            'status': order_status['status'],
                            'filled_quantity': order_status.get('filled', 0),
                            'remaining_quantity': order_status.get('remaining', 0),
                            'updated_at': datetime.now()
                        }}
                    )

                    logger.info(f"Order {order_id} completed with status: {order_status['status']}")

                    # Handle order group implications
                    group_id = order_info.get('group_id')
                    if group_id and group_id in self.order_groups:
                        await self._handle_order_completion_in_group(group_id, order_id, order_status, exchange)

            except Exception as e:
                log_exception_context(e, {'order_id': order_id})

        # Remove completed orders
        for order_id in orders_to_remove:
            del self.active_orders[order_id]

    async def _monitor_order_groups(self):
        """Monitor order groups for proper exit order management."""
        groups_to_remove = []

        for group_id, order_group in self.order_groups.items():
            try:
                if order_group.is_complete():
                    continue

                # Check if any exit orders need to be cancelled due to the other being filled
                await self._manage_exit_order_cancellation(order_group)

                # Check if group should be marked as complete
                active_orders = order_group.get_active_orders()
                if not active_orders:
                    order_group.update_state('completed')
                    groups_to_remove.append(group_id)
                    logger.info(f"Order group {group_id} completed")

            except Exception as e:
                log_exception_context(e, {'group_id': group_id})

        # Remove completed groups
        for group_id in groups_to_remove:
            del self.order_groups[group_id]

    async def _handle_order_completion_in_group(self, group_id: str, completed_order_id: str,
                                              order_status: Dict[str, Any], exchange):
        """Handle order completion within an order group."""
        try:
            order_group = self.order_groups.get(group_id)
            if not order_group:
                return

            # Find the completed order type
            completed_order_type = None
            for order_type, order in order_group.orders.items():
                if order and order.get('id') == completed_order_id:
                    completed_order_type = order_type
                    break

            if not completed_order_type:
                return

            # If an exit order was filled, cancel the other exit order
            if completed_order_type in ['stop_loss', 'take_profit'] and order_status['status'] == 'closed':
                await self._cancel_other_exit_orders(order_group, completed_order_type, exchange)

        except Exception as e:
            log_exception_context(e, {'group_id': group_id, 'completed_order_id': completed_order_id})

    async def _cancel_other_exit_orders(self, order_group: AtomicOrderGroup,
                                      filled_order_type: str, exchange):
        """Cancel other exit orders when one is filled."""
        try:
            symbol = order_group.symbol

            # Cancel the other exit order
            other_order_type = 'take_profit' if filled_order_type == 'stop_loss' else 'stop_loss'
            other_order = order_group.get_order(other_order_type)

            if other_order and other_order.get('id'):
                try:
                    await exchange.cancel_order(other_order['id'], symbol)
                    logger.info(f"Cancelled {other_order_type} order {other_order['id']} in group {order_group.group_id}")

                    # Update database
                    await self.db.orders.update_one(
                        {'order_id': other_order['id']},
                        {'$set': {
                            'status': 'canceled',
                            'updated_at': datetime.now(),
                            'cancellation_reason': f'Other exit order ({filled_order_type}) was filled'
                        }}
                    )

                except Exception as e:
                    logger.warning(f"Failed to cancel {other_order_type} order {other_order['id']}: {e}")

        except Exception as e:
            log_exception_context(e, {'group_id': order_group.group_id, 'filled_order_type': filled_order_type})

    async def _manage_exit_order_cancellation(self, order_group: AtomicOrderGroup):
        """Manage exit order cancellation logic for order groups."""
        # This method can be extended for more sophisticated exit order management
        # For now, the cancellation is handled in _handle_order_completion_in_group
        pass
    
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
            
            # Update database (async)
            await self.db.orders.update_one(
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
