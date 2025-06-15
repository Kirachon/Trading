"""
Portfolio Accounting Service
Tracks positions, balances, and P&L calculations.
"""
import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any
import asyncpg

# Add shared modules to path
sys.path.append('/app')
from shared.messaging import MessageConsumer, RabbitMQPublisher
from shared.outbox import OutboxRepository
from shared.exceptions import (
    TradingSystemError, DatabaseError, DatabaseConnectionError, DatabaseTimeoutError,
    DatabaseDeadlockError, DatabaseIntegrityError, PortfolioError, PortfolioCalculationError,
    InsufficientBalanceError, PositionNotFoundError, MessagingError, MessageConsumeError,
    RecoveryStrategy, log_exception_context
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PortfolioAccountingService:
    """Portfolio accounting service for tracking trades, positions, and P&L."""
    
    def __init__(self):
        self.postgres_url = os.getenv('POSTGRES_URL', 'postgresql://admin:SecureDB2024!@localhost:5432/trading')
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://localhost/')
        
        # Messaging components
        self.fill_consumer = MessageConsumer(self.rabbitmq_url)
        self.rabbitmq_publisher = RabbitMQPublisher(self.rabbitmq_url)
        self.command_consumer = MessageConsumer(self.rabbitmq_url)
        
        # Database connection pool
        self.db_pool = None
        self.outbox_repository = None
        
        # Portfolio state cache
        self.portfolio_cache = {
            'positions': {},
            'balances': {},
            'daily_pnl': {},
            'last_update': None
        }
        
        # Control flags
        self.running = False
        
    async def initialize(self):
        """Initialize the portfolio accounting service."""
        try:
            # Connect to messaging systems
            await self.fill_consumer.connect()
            await self.rabbitmq_publisher.connect()
            await self.command_consumer.connect()
            
            # Connect to PostgreSQL
            await self.connect_database()
            
            # Load initial portfolio state
            await self.load_portfolio_state()
            
            logger.info("Portfolio Accounting Service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Portfolio Accounting Service: {e}")
            raise
    
    async def connect_database(self):
        """Connect to PostgreSQL database using asyncpg with specific exception handling."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.db_pool = await asyncpg.create_pool(self.postgres_url)
                # Test connection
                async with self.db_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                logger.info("Connected to PostgreSQL database")

                # Initialize outbox repository
                self.outbox_repository = OutboxRepository(self.db_pool)
                await self.outbox_repository.create_table()
                logger.info("Outbox repository initialized")
                return

            except asyncpg.exceptions.InvalidCatalogNameError as e:
                log_exception_context(e, {'postgres_url': self.postgres_url})
                raise DatabaseConnectionError("Database does not exist", context={'postgres_url': self.postgres_url})

            except asyncpg.exceptions.InvalidPasswordError as e:
                log_exception_context(e, {'postgres_url': self.postgres_url})
                raise DatabaseConnectionError("Invalid database credentials", context={'postgres_url': self.postgres_url})

            except asyncpg.exceptions.ConnectionDoesNotExistError as e:
                if attempt < max_retries - 1:
                    delay = RecoveryStrategy.get_retry_delay(attempt)
                    logger.warning(f"Database connection failed, retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    log_exception_context(e, {'attempt': attempt + 1, 'max_retries': max_retries})
                    raise DatabaseConnectionError(
                        f"Failed to connect to PostgreSQL after {max_retries} attempts",
                        context={'postgres_url': self.postgres_url}
                    )

            except Exception as e:
                if attempt < max_retries - 1:
                    delay = RecoveryStrategy.get_retry_delay(attempt)
                    logger.warning(f"Database connection error, retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    log_exception_context(e, {'postgres_url': self.postgres_url})
                    raise DatabaseError(f"Unexpected database connection error: {str(e)}")
    
    async def load_portfolio_state(self):
        """Load current portfolio state from database."""
        try:
            async with self.db_pool.acquire() as conn:
                # Load current positions
                positions = await conn.fetch("""
                    SELECT exchange_id, symbol, side, quantity, avg_price,
                           unrealized_pnl, realized_pnl, strategy_id, opened_at
                    FROM positions
                    ORDER BY opened_at DESC
                """)

                # Load portfolio balances
                balances = await conn.fetch("""
                    SELECT exchange_id, asset, balance, locked, updated_at
                    FROM portfolio
                    ORDER BY updated_at DESC
                """)

                # Load today's P&L
                today = datetime.now().date()
                daily_stats = await conn.fetch("""
                    SELECT exchange_id, strategy_id,
                           SUM(realized_pnl) as daily_realized_pnl,
                           COUNT(*) as trade_count
                    FROM trades
                    WHERE DATE(created_at) = $1 AND status = 'filled'
                    GROUP BY exchange_id, strategy_id
                """, today)
            
                # Update cache
                self.portfolio_cache = {
                    'positions': {
                        f"{pos['exchange_id']}:{pos['symbol']}:{pos['strategy_id']}": dict(pos)
                        for pos in positions
                    },
                    'balances': {
                        f"{bal['exchange_id']}:{bal['asset']}": dict(bal)
                        for bal in balances
                    },
                    'daily_pnl': {
                        f"{stat['exchange_id']}:{stat['strategy_id']}": dict(stat)
                        for stat in daily_stats
                    },
                    'last_update': datetime.now()
                }

                logger.info(f"Loaded portfolio state: {len(positions)} positions, {len(balances)} balances")
            
        except Exception as e:
            logger.error(f"Error loading portfolio state: {e}")
            self.portfolio_cache = {
                'positions': {},
                'balances': {},
                'daily_pnl': {},
                'last_update': datetime.now()
            }
    
    async def process_fill_event(self, fill_event: Dict[str, Any]):
        """Process a fill event and update portfolio with optimized transaction management."""
        max_retries = 3
        retry_delay = 0.1  # Start with 100ms delay

        for attempt in range(max_retries):
            try:
                logger.info(f"Processing fill (attempt {attempt + 1}): {fill_event['side']} {fill_event['quantity']} {fill_event['symbol']}")

                # Use optimized transaction with deadlock handling
                async with self.db_pool.acquire() as conn:
                    # Set transaction isolation level to reduce deadlocks
                    await conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")

                    async with conn.transaction():
                        # Record the trade first (minimal lock time)
                        trade_id = await self.record_trade(conn, fill_event)

                        # Update position with row-level locking
                        await self.update_position_optimized(conn, fill_event)

                        # Update balances (separate from position to reduce lock contention)
                        await self.update_balances_optimized(conn, fill_event)

                        # Calculate and update P&L (read-heavy operation)
                        await self.calculate_pnl_optimized(conn, fill_event, trade_id)

                        # Update performance metrics (aggregation operation)
                        await self.update_performance_metrics_optimized(conn, fill_event)

                        # Add outbox message for portfolio update (within transaction)
                        await self.outbox_repository.add_message(
                            conn,
                            aggregate_id=str(trade_id),
                            aggregate_type="portfolio",
                            event_type="portfolio_updated",
                            payload={
                                'trade_id': trade_id,
                                'fill_event': fill_event,
                                'timestamp': datetime.now().isoformat()
                            }
                        )

                        # Transaction will auto-commit if no exceptions

                    # Update cache outside transaction
                    await self.refresh_portfolio_cache()

                    logger.info(f"Successfully processed fill event: {fill_event['order_id']}")
                    return  # Success, exit retry loop

            except asyncpg.exceptions.DeadlockDetectedError as e:
                logger.warning(f"Deadlock detected on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                    continue
                else:
                    logger.error(f"Failed to process fill after {max_retries} attempts due to deadlocks")
                    raise

            except asyncpg.exceptions.UniqueViolationError as e:
                logger.warning(f"Duplicate fill event detected: {fill_event['order_id']}")
                return  # Skip duplicate, don't retry

            except Exception as e:
                logger.error(f"Error in process_fill_event (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (2 ** attempt))
                    continue
                else:
                    raise
    
    async def record_trade(self, conn, fill_event: Dict[str, Any]) -> int:
        """Record trade in the trades table."""
        try:
            # Calculate commission (simplified)
            commission_rate = 0.001  # 0.1% default
            commission = float(fill_event['quantity']) * float(fill_event['price']) * commission_rate

            trade_id = await conn.fetchval("""
                INSERT INTO trades (
                    exchange_id, symbol, order_id, side, quantity, price,
                    timestamp, strategy_id, status, commission, realized_pnl
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
            """,
                fill_event['exchange'],
                fill_event['symbol'],
                fill_event['order_id'],
                fill_event['side'],
                float(fill_event['quantity']),
                float(fill_event['price']),
                fill_event['timestamp'],
                fill_event['strategy_id'],
                'filled',
                commission,
                0.0  # Will be calculated later
            )

            logger.debug(f"Recorded trade with ID: {trade_id}")
            return trade_id

        except Exception as e:
            logger.error(f"Error recording trade: {e}")
            raise

    async def update_position_optimized(self, conn, fill_event: Dict[str, Any]):
        """Update position with optimized locking and reduced contention."""
        try:
            exchange_id = fill_event['exchange']
            symbol = fill_event['symbol']
            strategy_id = fill_event['strategy_id']
            side = fill_event['side']
            quantity = float(fill_event['quantity'])
            price = float(fill_event['price'])

            # Use SELECT FOR UPDATE to prevent race conditions
            existing_position = await conn.fetchrow("""
                SELECT * FROM positions
                WHERE exchange_id = $1 AND symbol = $2 AND strategy_id = $3
                FOR UPDATE NOWAIT
            """, exchange_id, symbol, strategy_id)

            if existing_position:
                # Update existing position
                await self.update_existing_position_optimized(conn, existing_position, fill_event)
            else:
                # Create new position with INSERT ... ON CONFLICT
                await self.create_new_position_optimized(conn, fill_event)

        except asyncpg.exceptions.LockNotAvailableError:
            # Handle lock timeout gracefully
            logger.warning(f"Position lock timeout for {symbol}, retrying...")
            raise asyncpg.exceptions.DeadlockDetectedError("Position lock timeout")
        except Exception as e:
            logger.error(f"Error updating position: {e}")
            raise

    async def update_existing_position_optimized(self, conn, existing_position, fill_event: Dict[str, Any]):
        """Update existing position with optimized calculations."""
        try:
            side = fill_event['side']
            quantity = float(fill_event['quantity'])
            price = float(fill_event['price'])

            current_quantity = float(existing_position['quantity'])
            current_avg_price = float(existing_position['avg_price'])
            current_side = existing_position['side']

            # Calculate new position
            if current_side == 'long':
                if side == 'buy':
                    # Adding to long position
                    new_quantity = current_quantity + quantity
                    new_avg_price = ((current_quantity * current_avg_price) + (quantity * price)) / new_quantity
                    new_side = 'long'
                else:  # sell
                    if quantity >= current_quantity:
                        # Closing long position or going short
                        new_quantity = abs(quantity - current_quantity)
                        new_side = 'short' if quantity > current_quantity else 'flat'
                        new_avg_price = price if new_side == 'short' else current_avg_price
                    else:
                        # Reducing long position
                        new_quantity = current_quantity - quantity
                        new_side = 'long'
                        new_avg_price = current_avg_price
            else:  # current_side == 'short'
                if side == 'sell':
                    # Adding to short position
                    new_quantity = current_quantity + quantity
                    new_avg_price = ((current_quantity * current_avg_price) + (quantity * price)) / new_quantity
                    new_side = 'short'
                else:  # buy
                    if quantity >= current_quantity:
                        # Closing short position or going long
                        new_quantity = abs(quantity - current_quantity)
                        new_side = 'long' if quantity > current_quantity else 'flat'
                        new_avg_price = price if new_side == 'long' else current_avg_price
                    else:
                        # Reducing short position
                        new_quantity = current_quantity - quantity
                        new_side = 'short'
                        new_avg_price = current_avg_price

            # Update position in database
            if new_side == 'flat' or new_quantity == 0:
                # Close position
                await conn.execute("""
                    DELETE FROM positions
                    WHERE id = $1
                """, existing_position['id'])
                logger.info(f"Closed position: {fill_event['symbol']}")
            else:
                # Update position
                await conn.execute("""
                    UPDATE positions
                    SET quantity = $1, avg_price = $2, side = $3, updated_at = CURRENT_TIMESTAMP
                    WHERE id = $4
                """, new_quantity, new_avg_price, new_side, existing_position['id'])
                logger.info(f"Updated position: {fill_event['symbol']} {new_side} {new_quantity}")

        except Exception as e:
            logger.error(f"Error updating existing position: {e}")
            raise

    async def create_new_position_optimized(self, conn, fill_event: Dict[str, Any]):
        """Create a new position with conflict handling."""
        try:
            side = 'long' if fill_event['side'] == 'buy' else 'short'

            await conn.execute("""
                INSERT INTO positions (
                    exchange_id, symbol, side, quantity, avg_price,
                    strategy_id, opened_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (exchange_id, symbol, strategy_id) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    avg_price = EXCLUDED.avg_price,
                    side = EXCLUDED.side,
                    updated_at = CURRENT_TIMESTAMP
            """,
                fill_event['exchange'],
                fill_event['symbol'],
                side,
                float(fill_event['quantity']),
                float(fill_event['price']),
                fill_event['strategy_id'],
                datetime.fromtimestamp(fill_event['timestamp'] / 1000)
            )

            logger.info(f"Created new {side} position: {fill_event['symbol']}")

        except Exception as e:
            logger.error(f"Error creating new position: {e}")
            raise

    async def update_position(self, conn, fill_event: Dict[str, Any]):
        """Update position based on fill event."""
        try:
            exchange_id = fill_event['exchange']
            symbol = fill_event['symbol']
            strategy_id = fill_event['strategy_id']
            side = fill_event['side']
            quantity = float(fill_event['quantity'])
            price = float(fill_event['price'])

            # Check for existing position
            existing_position = await conn.fetchrow("""
                SELECT * FROM positions
                WHERE exchange_id = $1 AND symbol = $2 AND strategy_id = $3
            """, exchange_id, symbol, strategy_id)

            if existing_position:
                # Update existing position
                await self.update_existing_position(conn, existing_position, fill_event)
            else:
                # Create new position
                await self.create_new_position(conn, fill_event)

        except Exception as e:
            logger.error(f"Error updating position: {e}")
            raise
    
    async def update_existing_position(self, conn, existing_position, fill_event: Dict[str, Any]):
        """Update an existing position with new fill."""
        try:
            current_qty = float(existing_position['quantity'])
            current_avg_price = float(existing_position['avg_price'])
            fill_qty = float(fill_event['quantity'])
            fill_price = float(fill_event['price'])
            fill_side = fill_event['side']
            position_side = existing_position['side']

            # Determine if this is adding to or reducing the position
            if (position_side == 'long' and fill_side == 'buy') or (position_side == 'short' and fill_side == 'sell'):
                # Adding to position
                total_cost = (current_qty * current_avg_price) + (fill_qty * fill_price)
                new_qty = current_qty + fill_qty
                new_avg_price = total_cost / new_qty if new_qty > 0 else 0

                await conn.execute("""
                    UPDATE positions
                    SET quantity = $1, avg_price = $2, updated_at = CURRENT_TIMESTAMP
                    WHERE id = $3
                """, new_qty, new_avg_price, existing_position['id'])

            else:
                # Reducing position
                new_qty = current_qty - fill_qty

                if new_qty <= 0:
                    # Position closed
                    await conn.execute("""
                        DELETE FROM positions WHERE id = $1
                    """, existing_position['id'])
                    logger.info(f"Closed position: {existing_position['symbol']}")
                else:
                    # Partial reduction
                    await conn.execute("""
                        UPDATE positions
                        SET quantity = $1, updated_at = CURRENT_TIMESTAMP
                        WHERE id = $2
                    """, new_qty, existing_position['id'])

        except Exception as e:
            logger.error(f"Error updating existing position: {e}")
            raise
    
    async def create_new_position(self, conn, fill_event: Dict[str, Any]):
        """Create a new position from fill event."""
        try:
            side = 'long' if fill_event['side'] == 'buy' else 'short'

            await conn.execute("""
                INSERT INTO positions (
                    exchange_id, symbol, side, quantity, avg_price,
                    strategy_id, opened_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
                fill_event['exchange'],
                fill_event['symbol'],
                side,
                float(fill_event['quantity']),
                float(fill_event['price']),
                fill_event['strategy_id'],
                datetime.fromtimestamp(fill_event['timestamp'] / 1000)
            )

            logger.info(f"Created new {side} position: {fill_event['symbol']}")

        except Exception as e:
            logger.error(f"Error creating new position: {e}")
            raise
    
    async def update_balances(self, conn, fill_event: Dict[str, Any]):
        """Update portfolio balances based on fill."""
        try:
            exchange_id = fill_event['exchange']
            symbol = fill_event['symbol']
            side = fill_event['side']
            quantity = float(fill_event['quantity'])
            price = float(fill_event['price'])

            # Parse symbol to get base and quote assets
            base_asset, quote_asset = symbol.split('/')

            # Calculate trade value
            trade_value = quantity * price
            commission = trade_value * 0.001  # 0.1% commission

            if side == 'buy':
                # Buying: decrease quote asset, increase base asset
                await self.update_asset_balance(conn, exchange_id, quote_asset, -(trade_value + commission))
                await self.update_asset_balance(conn, exchange_id, base_asset, quantity)
            else:
                # Selling: increase quote asset, decrease base asset
                await self.update_asset_balance(conn, exchange_id, quote_asset, trade_value - commission)
                await self.update_asset_balance(conn, exchange_id, base_asset, -quantity)

        except Exception as e:
            logger.error(f"Error updating balances: {e}")
            # Don't raise - balance updates are less critical than position tracking

    async def update_balances_optimized(self, conn, fill_event: Dict[str, Any]):
        """Update balances with optimized batch operations."""
        try:
            exchange_id = fill_event['exchange']
            symbol = fill_event['symbol']
            side = fill_event['side']
            quantity = float(fill_event['quantity'])
            price = float(fill_event['price'])
            commission = float(fill_event.get('commission', 0))

            # Parse symbol to get base and quote assets
            if '/' in symbol:
                base_asset, quote_asset = symbol.split('/')
            else:
                # Fallback for symbols without separator
                base_asset = symbol[:3]
                quote_asset = symbol[3:]

            trade_value = quantity * price

            # Prepare balance updates
            balance_updates = []

            if side == 'buy':
                # Buying: decrease quote asset, increase base asset
                balance_updates.append((exchange_id, quote_asset, -(trade_value + commission)))
                balance_updates.append((exchange_id, base_asset, quantity))
            else:
                # Selling: increase quote asset, decrease base asset
                balance_updates.append((exchange_id, quote_asset, trade_value - commission))
                balance_updates.append((exchange_id, base_asset, -quantity))

            # Execute batch balance updates
            for exchange_id, asset, amount in balance_updates:
                await conn.execute("""
                    INSERT INTO portfolio (exchange_id, asset, balance)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (exchange_id, asset) DO UPDATE SET
                        balance = portfolio.balance + EXCLUDED.balance,
                        updated_at = CURRENT_TIMESTAMP
                """, exchange_id, asset, amount)

        except Exception as e:
            logger.error(f"Error updating balances optimized: {e}")
            # Don't raise - balance updates are less critical than position tracking
    
    async def update_asset_balance(self, conn, exchange_id: str, asset: str, change: float):
        """Update balance for a specific asset."""
        try:
            # Check if balance exists
            result = await conn.fetchrow("""
                SELECT balance FROM portfolio
                WHERE exchange_id = $1 AND asset = $2
            """, exchange_id, asset)

            if result:
                # Update existing balance
                new_balance = float(result['balance']) + change
                await conn.execute("""
                    UPDATE portfolio
                    SET balance = $1, updated_at = CURRENT_TIMESTAMP
                    WHERE exchange_id = $2 AND asset = $3
                """, new_balance, exchange_id, asset)
            else:
                # Create new balance entry
                await conn.execute("""
                    INSERT INTO portfolio (exchange_id, asset, balance)
                    VALUES ($1, $2, $3)
                """, exchange_id, asset, max(0, change))

        except Exception as e:
            logger.error(f"Error updating asset balance for {asset}: {e}")
    
    async def calculate_pnl(self, conn, fill_event: Dict[str, Any]):
        """Calculate realized P&L for the trade."""
        try:
            # Simplified P&L calculation
            # In a real system, this would use FIFO/LIFO accounting

            exchange_id = fill_event['exchange']
            symbol = fill_event['symbol']
            strategy_id = fill_event['strategy_id']

            # Get recent trades for this symbol/strategy to calculate P&L
            recent_trades = await conn.fetch("""
                SELECT * FROM trades
                WHERE exchange_id = $1 AND symbol = $2 AND strategy_id = $3
                ORDER BY timestamp DESC
                LIMIT 10
            """, exchange_id, symbol, strategy_id)

            # Simple P&L calculation (would be more complex in production)
            if len(recent_trades) >= 2:
                current_trade = recent_trades[0]
                previous_trade = recent_trades[1]

                if current_trade['side'] != previous_trade['side']:
                    # Opposite sides - calculate P&L
                    if current_trade['side'] == 'sell':
                        pnl = (float(current_trade['price']) - float(previous_trade['price'])) * float(current_trade['quantity'])
                    else:
                        pnl = (float(previous_trade['price']) - float(current_trade['price'])) * float(current_trade['quantity'])

                    # Update trade with realized P&L
                    await conn.execute("""
                        UPDATE trades
                        SET realized_pnl = $1
                        WHERE id = $2
                    """, pnl, current_trade['id'])

                    logger.info(f"Calculated P&L: {pnl:.2f} for trade {current_trade['id']}")

        except Exception as e:
            logger.error(f"Error calculating P&L: {e}")

    async def calculate_pnl_optimized(self, conn, fill_event: Dict[str, Any], trade_id: int):
        """Calculate P&L with optimized queries and caching."""
        try:
            # Simplified P&L calculation with better performance
            exchange_id = fill_event['exchange']
            symbol = fill_event['symbol']
            strategy_id = fill_event['strategy_id']
            side = fill_event['side']
            quantity = float(fill_event['quantity'])
            price = float(fill_event['price'])

            # Calculate realized P&L for closing trades
            realized_pnl = 0.0

            if side == 'sell':
                # For sell orders, calculate P&L against recent buy orders (FIFO)
                recent_buys = await conn.fetch("""
                    SELECT price, quantity FROM trades
                    WHERE exchange_id = $1 AND symbol = $2 AND strategy_id = $3
                    AND side = 'buy' AND status = 'filled'
                    AND created_at >= NOW() - INTERVAL '30 days'
                    ORDER BY created_at ASC
                    LIMIT 10
                """, exchange_id, symbol, strategy_id)

                if recent_buys:
                    # Simple average cost calculation
                    total_buy_value = sum(float(trade['price']) * float(trade['quantity']) for trade in recent_buys)
                    total_buy_quantity = sum(float(trade['quantity']) for trade in recent_buys)

                    if total_buy_quantity > 0:
                        avg_buy_price = total_buy_value / total_buy_quantity
                        realized_pnl = (price - avg_buy_price) * quantity

            # Update trade with calculated P&L
            await conn.execute("""
                UPDATE trades SET realized_pnl = $1 WHERE id = $2
            """, realized_pnl, trade_id)

            logger.debug(f"Calculated P&L: {realized_pnl} for trade {trade_id}")

        except Exception as e:
            logger.error(f"Error calculating optimized P&L: {e}")

    async def update_performance_metrics_optimized(self, conn, fill_event: Dict[str, Any]):
        """Update performance metrics with efficient aggregation."""
        try:
            strategy_id = fill_event['strategy_id']
            today = datetime.now().date()

            # Use efficient aggregation query
            await conn.execute("""
                INSERT INTO daily_performance (
                    strategy_id, date, trade_count, total_pnl, last_updated
                )
                SELECT
                    $1 as strategy_id,
                    $2 as date,
                    COUNT(*) as trade_count,
                    COALESCE(SUM(realized_pnl), 0) as total_pnl,
                    CURRENT_TIMESTAMP as last_updated
                FROM trades
                WHERE strategy_id = $1 AND DATE(created_at) = $2 AND status = 'filled'
                ON CONFLICT (strategy_id, date) DO UPDATE SET
                    trade_count = EXCLUDED.trade_count,
                    total_pnl = EXCLUDED.total_pnl,
                    last_updated = EXCLUDED.last_updated
            """, strategy_id, today)

        except Exception as e:
            logger.error(f"Error updating optimized performance metrics: {e}")

    async def update_performance_metrics(self, conn, fill_event: Dict[str, Any]):
        """Update daily performance metrics."""
        try:
            strategy_id = fill_event['strategy_id']
            today = datetime.now().date()

            # Get today's trades for this strategy
            metrics = await conn.fetchrow("""
                SELECT COUNT(*) as trade_count,
                       SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                       SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                       SUM(realized_pnl) as total_pnl,
                       AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl END) as avg_win,
                       AVG(CASE WHEN realized_pnl < 0 THEN realized_pnl END) as avg_loss
                FROM trades
                WHERE strategy_id = $1 AND DATE(created_at) = $2 AND status = 'filled'
            """, strategy_id, today)

            if metrics and metrics['trade_count'] > 0:
                win_rate = float(metrics['winning_trades'] or 0) / float(metrics['trade_count'])

                # Upsert performance metrics
                await conn.execute("""
                    INSERT INTO performance_metrics (
                        strategy_id, date, total_trades, winning_trades, losing_trades,
                        total_pnl, win_rate, avg_win, avg_loss
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (strategy_id, date)
                    DO UPDATE SET
                        total_trades = EXCLUDED.total_trades,
                        winning_trades = EXCLUDED.winning_trades,
                        losing_trades = EXCLUDED.losing_trades,
                        total_pnl = EXCLUDED.total_pnl,
                        win_rate = EXCLUDED.win_rate,
                        avg_win = EXCLUDED.avg_win,
                        avg_loss = EXCLUDED.avg_loss
                """,
                    strategy_id, today, metrics['trade_count'], metrics['winning_trades'],
                    metrics['losing_trades'], metrics['total_pnl'], win_rate,
                    metrics['avg_win'], metrics['avg_loss']
                )

        except Exception as e:
            logger.error(f"Error updating performance metrics: {e}")
    
    async def refresh_portfolio_cache(self):
        """Refresh the portfolio cache from database."""
        try:
            await self.load_portfolio_state()
        except Exception as e:
            logger.error(f"Error refreshing portfolio cache: {e}")
    

    
    async def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get current portfolio summary."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get total portfolio value (simplified)
                balance_result = await conn.fetchrow("""
                    SELECT SUM(balance) as total_balance
                    FROM portfolio
                    WHERE asset = 'USDT'
                """)
                total_balance = float(balance_result['total_balance'] or 0)

                # Get today's P&L
                today = datetime.now().date()
                pnl_result = await conn.fetchrow("""
                    SELECT SUM(realized_pnl) as daily_pnl
                    FROM trades
                    WHERE DATE(created_at) = $1 AND status = 'filled'
                """, today)
                daily_pnl = float(pnl_result['daily_pnl'] or 0)

                # Get position count
                pos_result = await conn.fetchrow("SELECT COUNT(*) as position_count FROM positions")
                position_count = int(pos_result['position_count'] or 0)

                return {
                    'total_balance': total_balance,
                    'daily_pnl': daily_pnl,
                    'position_count': position_count,
                    'last_update': datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"Error getting portfolio summary: {e}")
            return {
                'total_balance': 0.0,
                'daily_pnl': 0.0,
                'position_count': 0,
                'last_update': datetime.now().isoformat()
            }
    
    async def handle_command(self, command: Dict[str, Any]):
        """Handle portfolio accounting commands."""
        try:
            command_type = command.get('type')
            
            if command_type == 'get_portfolio_summary':
                summary = await self.get_portfolio_summary()
                await self.rabbitmq_publisher.publish(summary, 'portfolio.summary')
                
            elif command_type == 'refresh_portfolio':
                await self.refresh_portfolio_cache()
                logger.info("Portfolio cache refreshed")
                
            elif command_type == 'get_positions':
                positions = list(self.portfolio_cache['positions'].values())
                await self.rabbitmq_publisher.publish({'positions': positions}, 'portfolio.positions')
                
            elif command_type == 'get_balances':
                balances = list(self.portfolio_cache['balances'].values())
                await self.rabbitmq_publisher.publish({'balances': balances}, 'portfolio.balances')
                
            else:
                logger.warning(f"Unknown command type: {command_type}")
                
        except Exception as e:
            logger.error(f"Error handling command: {e}")
    
    async def run(self):
        """Main service loop."""
        self.running = True
        
        try:
            await self.initialize()
            
            # Start consuming fill events
            fill_task = asyncio.create_task(
                self.fill_consumer.consume(
                    'fills',
                    self.process_fill_event,
                    'trading.fills'
                )
            )
            
            # Start consuming commands
            command_task = asyncio.create_task(
                self.command_consumer.consume(
                    'portfolio-commands',
                    self.handle_command,
                    'commands.portfolio'
                )
            )
            
            logger.info("Portfolio Accounting Service started successfully")
            
            # Run both tasks
            await asyncio.gather(fill_task, command_task, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error in main service loop: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources."""
        self.running = False

        try:
            await self.fill_consumer.close()
            await self.rabbitmq_publisher.close()
            await self.command_consumer.close()

            if self.db_pool:
                await self.db_pool.close()

            logger.info("Portfolio Accounting Service cleanup completed")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    """Main entry point."""
    service = PortfolioAccountingService()
    
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
