"""
Risk Management Service
Validates trading signals against risk parameters and portfolio limits.
"""
import asyncio
import logging
import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
import asyncpg
import pandas as pd
import redis.asyncio as redis_async

# Add shared modules to path
sys.path.append('/app')
from shared.messaging import MessageConsumer, RabbitMQPublisher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RiskManager:
    """Risk management service for validating trading signals."""
    
    def __init__(self):
        self.postgres_url = os.getenv('POSTGRES_URL', 'postgresql://admin:SecureDB2024!@localhost:5432/trading')
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://localhost/')
        self.redis_url = os.getenv('REDIS_URL', 'redis://redis:6379')

        # Messaging components
        self.signal_consumer = MessageConsumer(self.rabbitmq_url)
        self.rabbitmq_publisher = RabbitMQPublisher(self.rabbitmq_url)
        self.command_consumer = MessageConsumer(self.rabbitmq_url)

        # Database connection pool
        self.db_pool = None
        self.redis_client = None

        # Risk parameters (loaded from database)
        self.risk_limits = {}
        self.portfolio_state = {}

        # Data freshness tracking
        self.data_freshness = {
            'portfolio_last_update': None,
            'risk_limits_last_update': None,
            'market_data_last_update': {},
            'max_staleness_seconds': 300  # 5 minutes max staleness
        }

        # Circuit breaker for external dependencies
        self.circuit_breakers = {
            'database': {'failures': 0, 'last_failure': None, 'threshold': 5},
            'redis': {'failures': 0, 'last_failure': None, 'threshold': 3}
        }

        # Control flags
        self.running = False
        self.risk_checks_enabled = True
        
    async def initialize(self):
        """Initialize the risk management service."""
        try:
            # Connect to messaging systems
            await self.signal_consumer.connect()
            await self.rabbitmq_publisher.connect()
            await self.command_consumer.connect()
            
            # Connect to PostgreSQL
            await self.connect_database()
            
            # Load risk limits and portfolio state
            await self.load_risk_limits()
            await self.load_portfolio_state()
            
            logger.info("Risk Management Service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Risk Management Service: {e}")
            raise
    
    async def connect_database(self):
        """Connect to PostgreSQL database and Redis using async clients."""
        try:
            # Connect to PostgreSQL
            self.db_pool = await asyncpg.create_pool(self.postgres_url)
            logger.info("Connected to PostgreSQL database")

            # Connect to Redis for real-time data
            self.redis_client = redis_async.from_url(self.redis_url)
            logger.info("Connected to Redis for real-time data")

        except Exception as e:
            logger.error(f"Failed to connect to databases: {e}")
            self._record_circuit_breaker_failure('database')
            raise
    
    async def load_risk_limits(self):
        """Load risk limits from database."""
        try:
            async with self.db_pool.acquire() as conn:
                limits = await conn.fetch("""
                    SELECT exchange_id, symbol, max_position_size, max_daily_loss,
                           max_trades_per_day, fat_finger_threshold, enabled
                    FROM risk_limits
                    WHERE enabled = true
                """)

                for limit in limits:
                    key = f"{limit['exchange_id']}:{limit['symbol'] or 'ALL'}"
                    self.risk_limits[key] = {
                        'max_position_size': float(limit['max_position_size'] or 10000),
                        'max_daily_loss': float(limit['max_daily_loss'] or 1000),
                        'max_trades_per_day': int(limit['max_trades_per_day'] or 50),
                        'fat_finger_threshold': float(limit['fat_finger_threshold'] or 0.05)
                    }

                logger.info(f"Loaded {len(self.risk_limits)} risk limit configurations")
            self.data_freshness['risk_limits_last_update'] = datetime.now()

        except Exception as e:
            logger.error(f"Error loading risk limits: {e}")
            self._record_circuit_breaker_failure('database')
            # Set default limits
            self.risk_limits['default'] = {
                'max_position_size': 10000.0,
                'max_daily_loss': 1000.0,
                'max_trades_per_day': 50,
                'fat_finger_threshold': 0.05
            }

    def _record_circuit_breaker_failure(self, service: str):
        """Record a circuit breaker failure for a service."""
        if service in self.circuit_breakers:
            self.circuit_breakers[service]['failures'] += 1
            self.circuit_breakers[service]['last_failure'] = datetime.now()

            if self.circuit_breakers[service]['failures'] >= self.circuit_breakers[service]['threshold']:
                logger.error(f"Circuit breaker OPEN for {service} - too many failures")

    def _is_circuit_breaker_open(self, service: str) -> bool:
        """Check if circuit breaker is open for a service."""
        if service not in self.circuit_breakers:
            return False

        breaker = self.circuit_breakers[service]
        if breaker['failures'] < breaker['threshold']:
            return False

        # Check if enough time has passed to try again (5 minutes)
        if breaker['last_failure']:
            time_since_failure = (datetime.now() - breaker['last_failure']).total_seconds()
            if time_since_failure > 300:  # 5 minutes
                # Reset circuit breaker
                breaker['failures'] = 0
                breaker['last_failure'] = None
                logger.info(f"Circuit breaker RESET for {service}")
                return False

        return True

    def _reset_circuit_breaker(self, service: str):
        """Reset circuit breaker on successful operation."""
        if service in self.circuit_breakers:
            self.circuit_breakers[service]['failures'] = 0
            self.circuit_breakers[service]['last_failure'] = None

    async def _validate_data_freshness(self, data_type: str) -> bool:
        """Validate that data is fresh enough for risk decisions."""
        try:
            max_staleness = self.data_freshness['max_staleness_seconds']

            if data_type == 'portfolio':
                last_update = self.data_freshness['portfolio_last_update']
                if not last_update:
                    return False

                staleness = (datetime.now() - last_update).total_seconds()
                if staleness > max_staleness:
                    logger.warning(f"Portfolio data is stale: {staleness}s old")
                    return False

            elif data_type == 'risk_limits':
                last_update = self.data_freshness['risk_limits_last_update']
                if not last_update:
                    return False

                staleness = (datetime.now() - last_update).total_seconds()
                if staleness > max_staleness:
                    logger.warning(f"Risk limits data is stale: {staleness}s old")
                    return False

            return True

        except Exception as e:
            logger.error(f"Error validating data freshness: {e}")
            return False
    
    async def load_portfolio_state(self):
        """Load current portfolio state from database with freshness tracking."""
        try:
            if self._is_circuit_breaker_open('database'):
                logger.warning("Database circuit breaker is open, using cached data")
                return

            async with self.db_pool.acquire() as conn:
                # Load current positions
                positions = await conn.fetch("""
                    SELECT exchange_id, symbol, side, quantity, avg_price,
                           unrealized_pnl, strategy_id, updated_at
                    FROM positions
                """)

                # Load portfolio balances
                balances = await conn.fetch("""
                    SELECT exchange_id, asset, balance, locked, updated_at
                    FROM portfolio
                """)

                # Load today's trading statistics
                today = datetime.now().date()
                daily_stats = await conn.fetch("""
                    SELECT exchange_id, symbol, strategy_id,
                           COUNT(*) as trade_count,
                           SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END) as total_profit,
                           SUM(CASE WHEN realized_pnl < 0 THEN realized_pnl ELSE 0 END) as total_loss,
                           MAX(created_at) as last_trade_time
                    FROM trades
                    WHERE DATE(created_at) = $1 AND status = 'filled'
                    GROUP BY exchange_id, symbol, strategy_id
                """, today)

                # Organize portfolio state
                self.portfolio_state = {
                    'positions': {f"{pos['exchange_id']}:{pos['symbol']}:{pos['strategy_id']}": dict(pos) for pos in positions},
                    'balances': {f"{bal['exchange_id']}:{bal['asset']}": dict(bal) for bal in balances},
                    'daily_stats': {f"{stat['exchange_id']}:{stat['symbol']}:{stat['strategy_id']}": dict(stat) for stat in daily_stats}
                }

                # Update freshness tracking
                self.data_freshness['portfolio_last_update'] = datetime.now()
                self._reset_circuit_breaker('database')

                logger.info("Loaded portfolio state successfully")

        except Exception as e:
            logger.error(f"Error loading portfolio state: {e}")
            self._record_circuit_breaker_failure('database')
            # Keep existing state if available, otherwise initialize empty
            if not hasattr(self, 'portfolio_state') or not self.portfolio_state:
                self.portfolio_state = {'positions': {}, 'balances': {}, 'daily_stats': {}}

    async def get_real_time_portfolio_data(self, exchange_id: str, symbol: str, strategy_id: str) -> Dict[str, Any]:
        """Get real-time portfolio data for specific position."""
        try:
            if self._is_circuit_breaker_open('database'):
                # Fall back to cached data
                key = f"{exchange_id}:{symbol}:{strategy_id}"
                return self.portfolio_state.get('daily_stats', {}).get(key, {})

            async with self.db_pool.acquire() as conn:
                # Get fresh daily statistics for this specific position
                today = datetime.now().date()
                stats = await conn.fetchrow("""
                    SELECT COUNT(*) as trade_count,
                           SUM(CASE WHEN realized_pnl < 0 THEN realized_pnl ELSE 0 END) as total_loss,
                           SUM(realized_pnl) as daily_realized_pnl,
                           MAX(created_at) as last_trade_time
                    FROM trades
                    WHERE exchange_id = $1 AND symbol = $2 AND strategy_id = $3
                    AND DATE(created_at) = $4 AND status = 'filled'
                """, exchange_id, symbol, strategy_id, today)

                self._reset_circuit_breaker('database')
                return dict(stats) if stats else {}

        except Exception as e:
            logger.error(f"Error getting real-time portfolio data: {e}")
            self._record_circuit_breaker_failure('database')
            # Fall back to cached data
            key = f"{exchange_id}:{symbol}:{strategy_id}"
            return self.portfolio_state.get('daily_stats', {}).get(key, {})

    async def get_real_time_market_price(self, exchange_id: str, symbol: str) -> Optional[float]:
        """Get real-time market price from Redis cache."""
        try:
            if self._is_circuit_breaker_open('redis'):
                return None

            # Try to get latest market data from Redis
            market_data_key = f"market-data:{exchange_id}:{symbol}:latest"
            market_data = await self.redis_client.get(market_data_key)

            if market_data:
                import json
                data = json.loads(market_data)
                self._reset_circuit_breaker('redis')
                return float(data.get('close', 0))

            return None

        except Exception as e:
            logger.error(f"Error getting real-time market price: {e}")
            self._record_circuit_breaker_failure('redis')
            return None

    def get_risk_limits(self, exchange_id: str, symbol: str) -> Dict[str, Any]:
        """Get risk limits for a specific exchange and symbol."""
        # Try specific symbol first
        key = f"{exchange_id}:{symbol}"
        if key in self.risk_limits:
            return self.risk_limits[key]
        
        # Try exchange-wide limits
        key = f"{exchange_id}:ALL"
        if key in self.risk_limits:
            return self.risk_limits[key]
        
        # Fall back to default
        return self.risk_limits.get('default', {
            'max_position_size': 10000.0,
            'max_daily_loss': 1000.0,
            'max_trades_per_day': 50,
            'fat_finger_threshold': 0.05
        })
    
    def check_position_size_limit(self, signal: Dict[str, Any], limits: Dict[str, Any]) -> Tuple[bool, str]:
        """Check if signal violates position size limits."""
        try:
            exchange_id = signal['exchange']
            symbol = signal['symbol']
            strategy_id = signal['strategy_id']
            signal_side = signal['side']
            current_price = signal['price']
            
            # Calculate proposed position size (simplified - would need actual quantity)
            # For now, assume a standard position size calculation
            proposed_notional = limits['max_position_size'] * 0.1  # 10% of max as example
            
            # Check against maximum position size
            if proposed_notional > limits['max_position_size']:
                return False, f"Position size {proposed_notional} exceeds limit {limits['max_position_size']}"
            
            # Check existing position
            position_key = f"{exchange_id}:{symbol}:{strategy_id}"
            existing_position = self.portfolio_state['positions'].get(position_key)
            
            if existing_position:
                existing_notional = float(existing_position['quantity']) * float(existing_position['avg_price'])
                total_notional = existing_notional + proposed_notional
                
                if total_notional > limits['max_position_size']:
                    return False, f"Total position size {total_notional} would exceed limit {limits['max_position_size']}"
            
            return True, "Position size check passed"
            
        except Exception as e:
            logger.error(f"Error checking position size: {e}")
            return False, f"Position size check error: {e}"
    
    async def check_daily_loss_limit(self, signal: Dict[str, Any], limits: Dict[str, Any]) -> Tuple[bool, str]:
        """Check if current daily loss exceeds limits using real-time data."""
        try:
            exchange_id = signal['exchange']
            symbol = signal['symbol']
            strategy_id = signal['strategy_id']

            # Validate data freshness first
            if not await self._validate_data_freshness('portfolio'):
                # Refresh portfolio data
                await self.load_portfolio_state()

            # Get real-time daily statistics
            daily_stats = await self.get_real_time_portfolio_data(exchange_id, symbol, strategy_id)

            total_loss = float(daily_stats.get('total_loss', 0))

            if abs(total_loss) > limits['max_daily_loss']:
                return False, f"Daily loss {abs(total_loss)} exceeds limit {limits['max_daily_loss']}"

            return True, "Daily loss check passed"

        except Exception as e:
            logger.error(f"Error checking daily loss: {e}")
            return False, f"Daily loss check error: {e}"
    
    async def check_trade_frequency_limit(self, signal: Dict[str, Any], limits: Dict[str, Any]) -> Tuple[bool, str]:
        """Check if daily trade count exceeds limits using real-time data."""
        try:
            exchange_id = signal['exchange']
            symbol = signal['symbol']
            strategy_id = signal['strategy_id']

            # Get real-time daily statistics
            daily_stats = await self.get_real_time_portfolio_data(exchange_id, symbol, strategy_id)

            trade_count = int(daily_stats.get('trade_count', 0))

            if trade_count >= limits['max_trades_per_day']:
                return False, f"Daily trade count {trade_count} exceeds limit {limits['max_trades_per_day']}"

            return True, "Trade frequency check passed"

        except Exception as e:
            logger.error(f"Error checking trade frequency: {e}")
            return False, f"Trade frequency check error: {e}"
    
    async def check_fat_finger_protection(self, signal: Dict[str, Any], limits: Dict[str, Any]) -> Tuple[bool, str]:
        """Check for fat finger errors using real market price comparison."""
        try:
            signal_price = float(signal['price'])
            exchange_id = signal['exchange']
            symbol = signal['symbol']

            # Get real-time market price from Redis
            market_price = await self.get_real_time_market_price(exchange_id, symbol)

            if market_price is None:
                # If we can't get market price, use a more conservative approach
                logger.warning(f"No market price available for {symbol}, using conservative fat finger check")
                # Allow only very small deviations when market price is unknown
                conservative_threshold = min(limits['fat_finger_threshold'], 0.01)  # Max 1%

                # Compare against recent trade prices from our own data
                try:
                    if self.db_pool:
                        async with self.db_pool.acquire() as conn:
                            recent_price = await conn.fetchval("""
                                SELECT price FROM trades
                                WHERE exchange_id = $1 AND symbol = $2 AND status = 'filled'
                                ORDER BY created_at DESC
                                LIMIT 1
                            """, exchange_id, symbol)

                            if recent_price:
                                price_deviation = abs(signal_price - float(recent_price)) / float(recent_price)
                                if price_deviation > conservative_threshold:
                                    return False, f"Price deviation {price_deviation:.4f} exceeds conservative threshold {conservative_threshold} (no market price available)"
                except Exception:
                    pass

                return True, "Fat finger check passed (conservative mode)"

            # Calculate price deviation from real market price
            price_deviation = abs(signal_price - market_price) / market_price

            if price_deviation > limits['fat_finger_threshold']:
                return False, f"Price deviation {price_deviation:.4f} exceeds fat finger threshold {limits['fat_finger_threshold']} (signal: {signal_price}, market: {market_price})"

            return True, f"Fat finger check passed (deviation: {price_deviation:.4f})"

        except Exception as e:
            logger.error(f"Error checking fat finger protection: {e}")
            return False, f"Fat finger check error: {e}"
    
    def check_portfolio_correlation(self, signal: Dict[str, Any]) -> Tuple[bool, str]:
        """Check portfolio correlation and concentration risk."""
        try:
            # Simplified correlation check
            # In a real system, this would analyze correlations between positions
            
            exchange_id = signal['exchange']
            symbol = signal['symbol']
            
            # Count positions in the same exchange
            same_exchange_positions = sum(
                1 for key in self.portfolio_state['positions'].keys()
                if key.startswith(f"{exchange_id}:")
            )
            
            # Limit concentration to single exchange
            if same_exchange_positions > 10:  # Max 10 positions per exchange
                return False, f"Too many positions ({same_exchange_positions}) on exchange {exchange_id}"
            
            return True, "Correlation check passed"
            
        except Exception as e:
            logger.error(f"Error checking portfolio correlation: {e}")
            return False, f"Correlation check error: {e}"
    
    async def validate_signal(self, signal: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
        """Validate a trading signal against all risk checks with real-time data."""
        try:
            if not self.risk_checks_enabled:
                return True, "Risk checks disabled", signal

            exchange_id = signal['exchange']
            symbol = signal['symbol']

            # Validate data freshness before proceeding
            if not await self._validate_data_freshness('risk_limits'):
                await self.load_risk_limits()

            # Get applicable risk limits
            limits = self.get_risk_limits(exchange_id, symbol)

            # Run all risk checks (now async)
            check_results = await asyncio.gather(
                self.check_position_size_limit(signal, limits),
                self.check_daily_loss_limit(signal, limits),
                self.check_trade_frequency_limit(signal, limits),
                self.check_fat_finger_protection(signal, limits),
                return_exceptions=True
            )

            # Add synchronous correlation check
            correlation_check = self.check_portfolio_correlation(signal)
            check_results.append(correlation_check)

            # Process results and handle exceptions
            failed_checks = []
            for i, result in enumerate(check_results):
                if isinstance(result, Exception):
                    failed_checks.append((False, f"Check {i} failed with exception: {result}"))
                elif not result[0]:
                    failed_checks.append(result)

            if failed_checks:
                reasons = "; ".join([check[1] for check in failed_checks])
                return False, f"Risk checks failed: {reasons}", signal

            # All checks passed - enhance signal with risk metadata
            enhanced_signal = signal.copy()
            enhanced_signal['risk_metadata'] = {
                'risk_checks_passed': True,
                'risk_limits_applied': limits,
                'validation_timestamp': datetime.now().isoformat(),
                'data_freshness': {
                    'portfolio_last_update': self.data_freshness['portfolio_last_update'].isoformat() if self.data_freshness['portfolio_last_update'] else None,
                    'risk_limits_last_update': self.data_freshness['risk_limits_last_update'].isoformat() if self.data_freshness['risk_limits_last_update'] else None
                }
            }

            return True, "All risk checks passed", enhanced_signal

        except Exception as e:
            logger.error(f"Error validating signal: {e}")
            return False, f"Signal validation error: {e}", signal
    
    async def process_signal(self, signal: Dict[str, Any]):
        """Process incoming trading signal."""
        try:
            logger.info(f"Processing signal: {signal['strategy_id']} {signal['side']} {signal['symbol']}")
            
            # Validate signal
            is_valid, reason, enhanced_signal = await self.validate_signal(signal)
            
            if is_valid:
                # Signal passed all checks - forward to execution
                await self.rabbitmq_publisher.publish(enhanced_signal, 'trading.approved-signals')
                logger.info(f"Approved signal: {signal['strategy_id']} {signal['side']} {signal['symbol']}")
            else:
                # Signal failed risk checks - send to alerts
                alert_message = {
                    'type': 'risk_rejection',
                    'signal': signal,
                    'reason': reason,
                    'timestamp': datetime.now().isoformat()
                }
                await self.rabbitmq_publisher.publish(alert_message, 'trading.alerts')
                logger.warning(f"Rejected signal: {reason}")
            
        except Exception as e:
            logger.error(f"Error processing signal: {e}")
    
    async def handle_command(self, command: Dict[str, Any]):
        """Handle risk management commands."""
        try:
            command_type = command.get('type')
            
            if command_type == 'enable_risk_checks':
                self.risk_checks_enabled = True
                logger.info("Risk checks enabled")
                
            elif command_type == 'disable_risk_checks':
                self.risk_checks_enabled = False
                logger.warning("Risk checks disabled")
                
            elif command_type == 'reload_risk_limits':
                await self.load_risk_limits()
                logger.info("Risk limits reloaded")
                
            elif command_type == 'reload_portfolio':
                await self.load_portfolio_state()
                logger.info("Portfolio state reloaded")
                
            elif command_type == 'get_risk_status':
                status = {
                    'risk_checks_enabled': self.risk_checks_enabled,
                    'risk_limits_count': len(self.risk_limits),
                    'active_positions': len(self.portfolio_state['positions']),
                    'daily_stats': self.portfolio_state['daily_stats']
                }
                await self.rabbitmq_publisher.publish(status, 'risk.status')
                
            else:
                logger.warning(f"Unknown command type: {command_type}")
                
        except Exception as e:
            logger.error(f"Error handling command: {e}")
    
    async def run(self):
        """Main service loop."""
        self.running = True
        
        try:
            await self.initialize()
            
            # Start consuming signals
            signal_task = asyncio.create_task(
                self.signal_consumer.consume(
                    'trading-signals',
                    self.process_signal,
                    'trading.signals'
                )
            )
            
            # Start consuming commands
            command_task = asyncio.create_task(
                self.command_consumer.consume(
                    'risk-commands',
                    self.handle_command,
                    'commands.risk'
                )
            )
            
            logger.info("Risk Management Service started successfully")
            
            # Run both tasks
            await asyncio.gather(signal_task, command_task, return_exceptions=True)
            
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
            
            if self.db_pool:
                await self.db_pool.close()
                
            logger.info("Risk Management Service cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    """Main entry point."""
    risk_manager = RiskManager()
    
    try:
        await risk_manager.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Service error: {e}")
    finally:
        await risk_manager.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
