"""
Portfolio Accounting Service
Tracks positions, balances, and P&L calculations.
"""
import asyncio
import logging
import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np

# Add shared modules to path
sys.path.append('/app')
from shared.messaging import MessageConsumer, RabbitMQPublisher
from shared.exchange_factory import get_exchange

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
        
        # Database connection
        self.db_conn = None
        
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
            self.connect_database()
            
            # Load initial portfolio state
            await self.load_portfolio_state()
            
            logger.info("Portfolio Accounting Service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Portfolio Accounting Service: {e}")
            raise
    
    def connect_database(self):
        """Connect to PostgreSQL database."""
        try:
            self.db_conn = psycopg2.connect(self.postgres_url)
            self.db_conn.autocommit = False  # Use transactions
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    async def load_portfolio_state(self):
        """Load current portfolio state from database."""
        try:
            cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)
            
            # Load current positions
            cursor.execute("""
                SELECT exchange_id, symbol, side, quantity, avg_price, 
                       unrealized_pnl, realized_pnl, strategy_id, opened_at
                FROM positions
                ORDER BY opened_at DESC
            """)
            positions = cursor.fetchall()
            
            # Load portfolio balances
            cursor.execute("""
                SELECT exchange_id, asset, balance, locked, updated_at
                FROM portfolio
                ORDER BY updated_at DESC
            """)
            balances = cursor.fetchall()
            
            # Load today's P&L
            today = datetime.now().date()
            cursor.execute("""
                SELECT exchange_id, strategy_id,
                       SUM(realized_pnl) as daily_realized_pnl,
                       COUNT(*) as trade_count
                FROM trades 
                WHERE DATE(created_at) = %s AND status = 'filled'
                GROUP BY exchange_id, strategy_id
            """, (today,))
            daily_stats = cursor.fetchall()
            
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
            
            cursor.close()
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
        """Process a fill event and update portfolio."""
        try:
            logger.info(f"Processing fill: {fill_event['side']} {fill_event['quantity']} {fill_event['symbol']}")
            
            # Start database transaction
            cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)
            
            try:
                # Record the trade
                trade_id = await self.record_trade(cursor, fill_event)
                
                # Update position
                await self.update_position(cursor, fill_event)
                
                # Update balances
                await self.update_balances(cursor, fill_event)
                
                # Calculate and update P&L
                await self.calculate_pnl(cursor, fill_event)
                
                # Update performance metrics
                await self.update_performance_metrics(cursor, fill_event)
                
                # Commit transaction
                self.db_conn.commit()
                
                # Update cache
                await self.refresh_portfolio_cache()
                
                # Publish portfolio update event
                await self.publish_portfolio_update(fill_event, trade_id)
                
                logger.info(f"Successfully processed fill event: {fill_event['order_id']}")
                
            except Exception as e:
                # Rollback transaction on error
                self.db_conn.rollback()
                logger.error(f"Error processing fill event, rolled back: {e}")
                raise
            finally:
                cursor.close()
                
        except Exception as e:
            logger.error(f"Error in process_fill_event: {e}")
    
    async def record_trade(self, cursor, fill_event: Dict[str, Any]) -> int:
        """Record trade in the trades table."""
        try:
            # Calculate commission (simplified)
            commission_rate = 0.001  # 0.1% default
            commission = float(fill_event['quantity']) * float(fill_event['price']) * commission_rate
            
            cursor.execute("""
                INSERT INTO trades (
                    exchange_id, symbol, order_id, side, quantity, price, 
                    timestamp, strategy_id, status, commission, realized_pnl
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
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
            ))
            
            trade_id = cursor.fetchone()['id']
            logger.debug(f"Recorded trade with ID: {trade_id}")
            return trade_id
            
        except Exception as e:
            logger.error(f"Error recording trade: {e}")
            raise
    
    async def update_position(self, cursor, fill_event: Dict[str, Any]):
        """Update position based on fill event."""
        try:
            exchange_id = fill_event['exchange']
            symbol = fill_event['symbol']
            strategy_id = fill_event['strategy_id']
            side = fill_event['side']
            quantity = float(fill_event['quantity'])
            price = float(fill_event['price'])
            
            # Check for existing position
            cursor.execute("""
                SELECT * FROM positions 
                WHERE exchange_id = %s AND symbol = %s AND strategy_id = %s
            """, (exchange_id, symbol, strategy_id))
            
            existing_position = cursor.fetchone()
            
            if existing_position:
                # Update existing position
                await self.update_existing_position(cursor, existing_position, fill_event)
            else:
                # Create new position
                await self.create_new_position(cursor, fill_event)
                
        except Exception as e:
            logger.error(f"Error updating position: {e}")
            raise
    
    async def update_existing_position(self, cursor, existing_position: Dict[str, Any], fill_event: Dict[str, Any]):
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
                
                cursor.execute("""
                    UPDATE positions 
                    SET quantity = %s, avg_price = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (new_qty, new_avg_price, existing_position['id']))
                
            else:
                # Reducing position
                new_qty = current_qty - fill_qty
                
                if new_qty <= 0:
                    # Position closed
                    cursor.execute("""
                        DELETE FROM positions WHERE id = %s
                    """, (existing_position['id'],))
                    logger.info(f"Closed position: {existing_position['symbol']}")
                else:
                    # Partial reduction
                    cursor.execute("""
                        UPDATE positions 
                        SET quantity = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (new_qty, existing_position['id']))
                
        except Exception as e:
            logger.error(f"Error updating existing position: {e}")
            raise
    
    async def create_new_position(self, cursor, fill_event: Dict[str, Any]):
        """Create a new position from fill event."""
        try:
            side = 'long' if fill_event['side'] == 'buy' else 'short'
            
            cursor.execute("""
                INSERT INTO positions (
                    exchange_id, symbol, side, quantity, avg_price, 
                    strategy_id, opened_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                fill_event['exchange'],
                fill_event['symbol'],
                side,
                float(fill_event['quantity']),
                float(fill_event['price']),
                fill_event['strategy_id'],
                datetime.fromtimestamp(fill_event['timestamp'] / 1000)
            ))
            
            logger.info(f"Created new {side} position: {fill_event['symbol']}")
            
        except Exception as e:
            logger.error(f"Error creating new position: {e}")
            raise
    
    async def update_balances(self, cursor, fill_event: Dict[str, Any]):
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
                await self.update_asset_balance(cursor, exchange_id, quote_asset, -(trade_value + commission))
                await self.update_asset_balance(cursor, exchange_id, base_asset, quantity)
            else:
                # Selling: increase quote asset, decrease base asset
                await self.update_asset_balance(cursor, exchange_id, quote_asset, trade_value - commission)
                await self.update_asset_balance(cursor, exchange_id, base_asset, -quantity)
                
        except Exception as e:
            logger.error(f"Error updating balances: {e}")
            # Don't raise - balance updates are less critical than position tracking
    
    async def update_asset_balance(self, cursor, exchange_id: str, asset: str, change: float):
        """Update balance for a specific asset."""
        try:
            # Check if balance exists
            cursor.execute("""
                SELECT balance FROM portfolio 
                WHERE exchange_id = %s AND asset = %s
            """, (exchange_id, asset))
            
            result = cursor.fetchone()
            
            if result:
                # Update existing balance
                new_balance = float(result['balance']) + change
                cursor.execute("""
                    UPDATE portfolio 
                    SET balance = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE exchange_id = %s AND asset = %s
                """, (new_balance, exchange_id, asset))
            else:
                # Create new balance entry
                cursor.execute("""
                    INSERT INTO portfolio (exchange_id, asset, balance)
                    VALUES (%s, %s, %s)
                """, (exchange_id, asset, max(0, change)))
                
        except Exception as e:
            logger.error(f"Error updating asset balance for {asset}: {e}")
    
    async def calculate_pnl(self, cursor, fill_event: Dict[str, Any]):
        """Calculate realized P&L for the trade."""
        try:
            # Simplified P&L calculation
            # In a real system, this would use FIFO/LIFO accounting
            
            exchange_id = fill_event['exchange']
            symbol = fill_event['symbol']
            strategy_id = fill_event['strategy_id']
            
            # Get recent trades for this symbol/strategy to calculate P&L
            cursor.execute("""
                SELECT * FROM trades 
                WHERE exchange_id = %s AND symbol = %s AND strategy_id = %s
                ORDER BY timestamp DESC
                LIMIT 10
            """, (exchange_id, symbol, strategy_id))
            
            recent_trades = cursor.fetchall()
            
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
                    cursor.execute("""
                        UPDATE trades 
                        SET realized_pnl = %s
                        WHERE id = %s
                    """, (pnl, current_trade['id']))
                    
                    logger.info(f"Calculated P&L: {pnl:.2f} for trade {current_trade['id']}")
                    
        except Exception as e:
            logger.error(f"Error calculating P&L: {e}")
    
    async def update_performance_metrics(self, cursor, fill_event: Dict[str, Any]):
        """Update daily performance metrics."""
        try:
            strategy_id = fill_event['strategy_id']
            today = datetime.now().date()
            
            # Get today's trades for this strategy
            cursor.execute("""
                SELECT COUNT(*) as trade_count,
                       SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                       SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                       SUM(realized_pnl) as total_pnl,
                       AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl END) as avg_win,
                       AVG(CASE WHEN realized_pnl < 0 THEN realized_pnl END) as avg_loss
                FROM trades 
                WHERE strategy_id = %s AND DATE(created_at) = %s AND status = 'filled'
            """, (strategy_id, today))
            
            metrics = cursor.fetchone()
            
            if metrics and metrics['trade_count'] > 0:
                win_rate = float(metrics['winning_trades'] or 0) / float(metrics['trade_count'])
                
                # Upsert performance metrics
                cursor.execute("""
                    INSERT INTO performance_metrics (
                        strategy_id, date, total_trades, winning_trades, losing_trades,
                        total_pnl, win_rate, avg_win, avg_loss
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (strategy_id, date) 
                    DO UPDATE SET
                        total_trades = EXCLUDED.total_trades,
                        winning_trades = EXCLUDED.winning_trades,
                        losing_trades = EXCLUDED.losing_trades,
                        total_pnl = EXCLUDED.total_pnl,
                        win_rate = EXCLUDED.win_rate,
                        avg_win = EXCLUDED.avg_win,
                        avg_loss = EXCLUDED.avg_loss
                """, (
                    strategy_id, today, metrics['trade_count'], metrics['winning_trades'],
                    metrics['losing_trades'], metrics['total_pnl'], win_rate,
                    metrics['avg_win'], metrics['avg_loss']
                ))
                
        except Exception as e:
            logger.error(f"Error updating performance metrics: {e}")
    
    async def refresh_portfolio_cache(self):
        """Refresh the portfolio cache from database."""
        try:
            await self.load_portfolio_state()
        except Exception as e:
            logger.error(f"Error refreshing portfolio cache: {e}")
    
    async def publish_portfolio_update(self, fill_event: Dict[str, Any], trade_id: int):
        """Publish portfolio update event."""
        try:
            update_event = {
                'type': 'portfolio_update',
                'trade_id': trade_id,
                'fill_event': fill_event,
                'timestamp': datetime.now().isoformat(),
                'portfolio_summary': await self.get_portfolio_summary()
            }
            
            await self.rabbitmq_publisher.publish(update_event, 'portfolio.updates')
            
        except Exception as e:
            logger.error(f"Error publishing portfolio update: {e}")
    
    async def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get current portfolio summary."""
        try:
            cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)
            
            # Get total portfolio value (simplified)
            cursor.execute("""
                SELECT SUM(balance) as total_balance
                FROM portfolio 
                WHERE asset = 'USDT'
            """)
            balance_result = cursor.fetchone()
            total_balance = float(balance_result['total_balance'] or 0)
            
            # Get today's P&L
            today = datetime.now().date()
            cursor.execute("""
                SELECT SUM(realized_pnl) as daily_pnl
                FROM trades 
                WHERE DATE(created_at) = %s AND status = 'filled'
            """, (today,))
            pnl_result = cursor.fetchone()
            daily_pnl = float(pnl_result['daily_pnl'] or 0)
            
            # Get position count
            cursor.execute("SELECT COUNT(*) as position_count FROM positions")
            pos_result = cursor.fetchone()
            position_count = int(pos_result['position_count'] or 0)
            
            cursor.close()
            
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
            
            if self.db_conn:
                self.db_conn.close()
                
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
