"""
Strategy Engine Service
Orchestrates multiple trading strategies and generates signals.
"""
import asyncio
import logging
import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
import pandas as pd
import pymongo
from pymongo import MongoClient

# Add shared modules to path
sys.path.append('/app')
from shared.messaging import RedisSubscriber, RabbitMQPublisher, MessageConsumer
from shared.rolling_window import RollingWindow, OHLCVPoint, IncrementalIndicators
from shared.exceptions import (
    TradingSystemError, DatabaseError, DatabaseConnectionError, DatabaseTimeoutError,
    MarketDataError, MarketDataStaleError, StrategyError, StrategyCalculationError,
    InsufficientDataError, MessagingError, MessageConsumeError, MessagePublishError,
    RecoveryStrategy, log_exception_context
)

# Import all strategies
from strategies.moving_average import MovingAverageCrossover
from strategies.maee_formula import MAEEFormulaStrategy
from strategies.rsi import RSIStrategy
from strategies.macd import MACDStrategy
from strategies.bollinger_bands import BollingerBandsStrategy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StrategyEngine:
    """Strategy engine that manages multiple trading strategies."""
    
    def __init__(self):
        self.redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://localhost/')
        self.mongodb_url = os.getenv('MONGODB_URL', 'mongodb://localhost:27017')
        
        # Messaging components
        self.redis_subscriber = RedisSubscriber(self.redis_url)
        self.rabbitmq_publisher = RabbitMQPublisher(self.rabbitmq_url)
        self.command_consumer = MessageConsumer(self.rabbitmq_url)
        
        # MongoDB for historical data
        self.mongo_client = None
        self.db = None
        
        # Strategy instances
        self.strategies = {}
        self.strategy_configs = {}

        # Efficient rolling window data buffers for each symbol
        self.data_buffers = {}  # Dict[str, RollingWindow]
        self.buffer_size = 500  # Keep last 500 candles

        # Incremental indicators for each buffer
        self.incremental_indicators = {}  # Dict[str, IncrementalIndicators]

        # Control flags
        self.running = False
        self.strategies_enabled = True
        
    async def initialize(self):
        """Initialize the strategy engine with specific exception handling."""
        try:
            # Connect to messaging systems with specific error handling
            try:
                await self.redis_subscriber.connect()
            except Exception as e:
                log_exception_context(e, {'service': 'redis_subscriber'})
                raise DatabaseConnectionError("Failed to connect Redis subscriber", context={'redis_url': self.redis_url})

            try:
                await self.rabbitmq_publisher.connect()
            except Exception as e:
                log_exception_context(e, {'service': 'rabbitmq_publisher'})
                raise MessagingError("Failed to connect RabbitMQ publisher", context={'rabbitmq_url': self.rabbitmq_url})

            try:
                await self.command_consumer.connect()
            except Exception as e:
                log_exception_context(e, {'service': 'rabbitmq_consumer'})
                raise MessagingError("Failed to connect RabbitMQ consumer", context={'rabbitmq_url': self.rabbitmq_url})

            # Connect to MongoDB with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.mongo_client = MongoClient(self.mongodb_url)
                    self.db = self.mongo_client['trading_data']
                    # Test connection
                    self.mongo_client.admin.command('ping')
                    break
                except pymongo.errors.ServerSelectionTimeoutError as e:
                    if attempt < max_retries - 1:
                        delay = RecoveryStrategy.get_retry_delay(attempt)
                        logger.warning(f"MongoDB connection timeout, retrying in {delay}s...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        log_exception_context(e, {'attempt': attempt + 1, 'max_retries': max_retries})
                        raise DatabaseConnectionError(
                            f"Failed to connect to MongoDB after {max_retries} attempts",
                            context={'mongodb_url': self.mongodb_url}
                        )
                except Exception as e:
                    log_exception_context(e, {'service': 'mongodb'})
                    raise DatabaseError("Failed to connect to MongoDB", context={'mongodb_url': self.mongodb_url})

            # Load strategy configurations
            await self.load_strategy_configs()

            # Initialize strategies
            self.initialize_strategies()

            logger.info("Strategy Engine initialized successfully")

        except TradingSystemError:
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            log_exception_context(e, {'component': 'strategy_engine_init'})
            raise TradingSystemError(f"Failed to initialize Strategy Engine: {str(e)}")
    
    async def load_strategy_configs(self):
        """Load strategy configurations from database or config file."""
        try:
            # Default strategy configurations
            default_configs = {
                'ma_crossover': {
                    'enabled': True,
                    'params': {
                        'short_window': 50,
                        'long_window': 200,
                        'ma_type': 'sma'
                    }
                },
                'maee_formula': {
                    'enabled': True,
                    'params': {
                        'ma_period': 200,
                        'atr_period': 14,
                        'rr_ratio': 2.0,
                        'swing_lookback': 20
                    }
                },
                'rsi_strategy': {
                    'enabled': True,
                    'params': {
                        'rsi_period': 14,
                        'oversold': 30,
                        'overbought': 70
                    }
                },
                'macd_strategy': {
                    'enabled': True,
                    'params': {
                        'fast_period': 12,
                        'slow_period': 26,
                        'signal_period': 9
                    }
                },
                'bollinger_bands': {
                    'enabled': True,
                    'params': {
                        'period': 20,
                        'std_dev': 2.0,
                        'mean_reversion_mode': True
                    }
                }
            }
            
            self.strategy_configs = default_configs
            logger.info(f"Loaded {len(self.strategy_configs)} strategy configurations")
            
        except Exception as e:
            logger.error(f"Error loading strategy configs: {e}")
            self.strategy_configs = {}
    
    def initialize_strategies(self):
        """Initialize strategy instances based on configurations."""
        try:
            strategy_classes = {
                'ma_crossover': MovingAverageCrossover,
                'maee_formula': MAEEFormulaStrategy,
                'rsi_strategy': RSIStrategy,
                'macd_strategy': MACDStrategy,
                'bollinger_bands': BollingerBandsStrategy
            }
            
            for strategy_id, config in self.strategy_configs.items():
                if config.get('enabled', False) and strategy_id in strategy_classes:
                    try:
                        strategy_class = strategy_classes[strategy_id]
                        strategy = strategy_class(strategy_id, config['params'])
                        self.strategies[strategy_id] = strategy
                        logger.info(f"Initialized strategy: {strategy_id}")
                    except Exception as e:
                        logger.error(f"Failed to initialize strategy {strategy_id}: {e}")
            
            logger.info(f"Initialized {len(self.strategies)} strategies")
            
        except Exception as e:
            logger.error(f"Error initializing strategies: {e}")
    
    async def process_market_data(self, data: Dict[str, Any], channel: str):
        """Process incoming market data and generate signals using efficient rolling windows."""
        try:
            if not self.strategies_enabled:
                return

            symbol = data.get('symbol')
            exchange = data.get('exchange')
            timeframe = data.get('timeframe')

            if not all([symbol, exchange, timeframe]):
                logger.warning(f"Incomplete market data: {data}")
                return

            # Create buffer key
            buffer_key = f"{exchange}:{symbol}:{timeframe}"

            # Initialize rolling window buffer if needed
            if buffer_key not in self.data_buffers:
                self.data_buffers[buffer_key] = RollingWindow(max_size=self.buffer_size)
                self.incremental_indicators[buffer_key] = IncrementalIndicators()

            # Create efficient OHLCV data point
            try:
                data_point = OHLCVPoint(
                    timestamp=data['timestamp'],
                    datetime_obj=pd.to_datetime(data['datetime']),
                    open_price=float(data['open']),
                    high=float(data['high']),
                    low=float(data['low']),
                    close=float(data['close']),
                    volume=float(data['volume'])
                )
            except (ValueError, TypeError) as e:
                log_exception_context(e, {'data': data, 'symbol': symbol})
                raise MarketDataError(f"Invalid market data format for {symbol}", context={'data': data})
            except KeyError as e:
                log_exception_context(e, {'data': data, 'symbol': symbol})
                raise MarketDataError(f"Missing required field in market data: {e}", context={'data': data})

            # Add to rolling window (O(1) operation)
            rolling_window = self.data_buffers[buffer_key]
            if not rolling_window.append(data_point):
                return  # Duplicate or invalid timestamp

            # Only process if we have sufficient data
            if not rolling_window.is_ready(min_points=50):
                return

            # Update incremental indicators efficiently
            indicators = self.incremental_indicators[buffer_key]
            self._update_incremental_indicators(rolling_window, indicators, buffer_key)

            # Generate signals for each strategy (using optimized data)
            await self.generate_strategy_signals_optimized(rolling_window, symbol, exchange, timeframe)

        except Exception as e:
            logger.error(f"Error processing market data for {symbol}: {e}")
            # Log additional context for debugging
            logger.error(f"Data: {data}")
            logger.error(f"Channel: {channel}")

    def _update_incremental_indicators(self, rolling_window: RollingWindow,
                                     indicators: IncrementalIndicators, buffer_key: str):
        """Update incremental indicators efficiently."""
        try:
            # Update common indicators used by strategies
            indicators.update_sma(rolling_window, 10, f"{buffer_key}_sma_10")
            indicators.update_sma(rolling_window, 20, f"{buffer_key}_sma_20")
            indicators.update_sma(rolling_window, 50, f"{buffer_key}_sma_50")
            indicators.update_ema(rolling_window, 12, f"{buffer_key}_ema_12")
            indicators.update_ema(rolling_window, 26, f"{buffer_key}_ema_26")

            # Mark last update time
            indicators.last_update = datetime.now()

        except Exception as e:
            logger.error(f"Error updating incremental indicators: {e}")

    async def generate_strategy_signals_optimized(self, rolling_window: RollingWindow,
                                                symbol: str, exchange: str, timeframe: str):
        """Generate signals using optimized rolling window data."""
        try:
            current_time = datetime.now()
            latest_point = rolling_window.get_latest()
            if not latest_point:
                return

            current_price = latest_point.close

            for strategy_id, strategy in self.strategies.items():
                try:
                    # Check if strategy supports optimized processing
                    if hasattr(strategy, 'generate_signals_optimized'):
                        # Use optimized method with rolling window
                        signals = strategy.generate_signals_optimized(rolling_window)
                    else:
                        # Fall back to DataFrame conversion for compatibility
                        df = rolling_window.to_dataframe()
                        signals = strategy.generate_signals(df)

                    if signals is None or (hasattr(signals, 'empty') and signals.empty):
                        continue

                    # Handle both Series and scalar signal results
                    if hasattr(signals, 'iloc'):
                        # Series result
                        latest_signal = signals.iloc[-1]
                    else:
                        # Scalar result
                        latest_signal = signals

                    # Process signal if significant
                    if abs(latest_signal) > 0.1 and strategy.should_generate_signal(latest_signal):
                        await self._process_strategy_signal(
                            strategy, strategy_id, latest_signal, current_price,
                            symbol, exchange, timeframe, current_time, rolling_window
                        )

                except InsufficientDataError as e:
                    # Expected error when not enough data - don't log as error
                    logger.debug(f"Insufficient data for strategy {strategy_id}: {e}")
                    continue
                except StrategyCalculationError as e:
                    log_exception_context(e, {'strategy_id': strategy_id, 'symbol': symbol})
                    continue
                except Exception as e:
                    log_exception_context(e, {'strategy_id': strategy_id, 'symbol': symbol})
                    raise StrategyError(f"Unexpected error in strategy {strategy_id}", context={'symbol': symbol})

        except Exception as e:
            logger.error(f"Error in optimized signal generation: {e}")

    async def _process_strategy_signal(self, strategy, strategy_id: str, signal: float,
                                     current_price: float, symbol: str, exchange: str,
                                     timeframe: str, current_time: datetime,
                                     rolling_window: RollingWindow):
        """Process and publish a strategy signal."""
        try:
            signal_message = {
                'strategy_id': strategy_id,
                'symbol': symbol,
                'exchange': exchange,
                'timeframe': timeframe,
                'side': 'buy' if signal > 0 else 'sell',
                'signal_strength': abs(signal),
                'price': current_price,
                'timestamp': int(current_time.timestamp() * 1000),
                'datetime': current_time.isoformat()
            }

            # Calculate stop loss and take profit if strategy supports it
            if hasattr(strategy, 'calculate_exits'):
                try:
                    if hasattr(strategy, 'calculate_exits_optimized'):
                        # Use optimized method
                        sl, tp = strategy.calculate_exits_optimized(rolling_window, signal)
                    else:
                        # Fall back to DataFrame method
                        df = rolling_window.to_dataframe()
                        signals_series = pd.Series([signal], index=[rolling_window.get_latest().datetime])
                        sl, tp = strategy.calculate_exits(df, signals_series)
                        sl = sl.iloc[-1] if not sl.empty and not pd.isna(sl.iloc[-1]) else None
                        tp = tp.iloc[-1] if not tp.empty and not pd.isna(tp.iloc[-1]) else None

                    signal_message['stop_loss'] = float(sl) if sl is not None else None
                    signal_message['take_profit'] = float(tp) if tp is not None else None

                except Exception as e:
                    logger.warning(f"Error calculating exits for {strategy_id}: {e}")

            # Publish signal to RabbitMQ with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await self.rabbitmq_publisher.publish(signal_message, 'trading.signals')
                    break
                except Exception as e:
                    if RecoveryStrategy.should_retry(e, attempt, max_retries):
                        delay = RecoveryStrategy.get_retry_delay(attempt)
                        logger.warning(f"Failed to publish signal, retrying in {delay}s...")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        log_exception_context(e, {'signal_message': signal_message})
                        raise MessagePublishError(f"Failed to publish signal after {max_retries} attempts")

            # Update strategy state
            try:
                strategy.update_position(signal)
            except Exception as e:
                log_exception_context(e, {'strategy_id': strategy_id, 'signal': signal})
                # Don't fail the entire process for state update errors
                logger.warning(f"Failed to update strategy state for {strategy_id}: {e}")

            logger.info(f"Generated {signal_message['side']} signal for {symbol} "
                       f"from {strategy_id} at {current_price} (strength: {abs(signal):.3f})")

        except TradingSystemError:
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            log_exception_context(e, {'strategy_id': strategy_id, 'symbol': symbol})
            raise StrategyError(f"Unexpected error processing signal from {strategy_id}")

    async def generate_strategy_signals(self, market_data: pd.DataFrame,
                                      symbol: str, exchange: str, timeframe: str):
        """Generate signals from all enabled strategies."""
        try:
            current_time = datetime.now()
            current_price = market_data['close'].iloc[-1]
            
            for strategy_id, strategy in self.strategies.items():
                try:
                    # Generate signals
                    signals = strategy.generate_signals(market_data)
                    
                    if signals.empty:
                        continue
                    
                    # Get the latest signal
                    latest_signal = signals.iloc[-1]
                    
                    if latest_signal != 0:  # Non-zero signal
                        # Check if we should generate this signal
                        if strategy.should_generate_signal(latest_signal):
                            # Create signal message
                            signal_message = {
                                'strategy_id': strategy_id,
                                'strategy_name': strategy.name,
                                'symbol': symbol,
                                'exchange': exchange,
                                'timeframe': timeframe,
                                'signal': float(latest_signal),
                                'side': 'buy' if latest_signal > 0 else 'sell',
                                'price': current_price,
                                'timestamp': int(current_time.timestamp() * 1000),
                                'datetime': current_time.isoformat(),
                                'confidence': abs(latest_signal),
                                'metadata': {
                                    'indicator_values': strategy.get_indicator_values(market_data),
                                    'strategy_description': strategy.get_strategy_description()
                                }
                            }
                            
                            # Calculate stop loss and take profit if strategy supports it
                            if hasattr(strategy, 'calculate_exits'):
                                try:
                                    sl, tp = strategy.calculate_exits(market_data, signals)
                                    if not sl.empty and not tp.empty:
                                        signal_message['stop_loss'] = float(sl.iloc[-1]) if not pd.isna(sl.iloc[-1]) else None
                                        signal_message['take_profit'] = float(tp.iloc[-1]) if not pd.isna(tp.iloc[-1]) else None
                                except Exception as e:
                                    logger.warning(f"Error calculating exits for {strategy_id}: {e}")
                            
                            # Publish signal to RabbitMQ
                            await self.rabbitmq_publisher.publish(
                                signal_message, 
                                'trading.signals'
                            )
                            
                            # Update strategy state
                            strategy.update_position(latest_signal)
                            
                            logger.info(f"Generated {signal_message['side']} signal for {symbol} "
                                      f"from {strategy_id} at {current_price}")
                
                except Exception as e:
                    logger.error(f"Error generating signals for strategy {strategy_id}: {e}")
        
        except Exception as e:
            logger.error(f"Error in generate_strategy_signals: {e}")
    
    async def handle_command(self, command: Dict[str, Any]):
        """Handle commands from UI or other services."""
        try:
            command_type = command.get('type')
            
            if command_type == 'enable_strategies':
                self.strategies_enabled = True
                logger.info("Strategies enabled")
                
            elif command_type == 'disable_strategies':
                self.strategies_enabled = False
                logger.info("Strategies disabled")
                
            elif command_type == 'reload_strategies':
                await self.load_strategy_configs()
                self.initialize_strategies()
                logger.info("Strategies reloaded")
                
            elif command_type == 'update_strategy_config':
                strategy_id = command.get('strategy_id')
                new_config = command.get('config')
                
                if strategy_id and new_config:
                    self.strategy_configs[strategy_id] = new_config
                    # Reinitialize the specific strategy
                    if strategy_id in self.strategies:
                        del self.strategies[strategy_id]
                    self.initialize_strategies()
                    logger.info(f"Updated configuration for strategy {strategy_id}")
                    
            elif command_type == 'get_strategy_status':
                # Return strategy status
                status = {
                    'strategies_enabled': self.strategies_enabled,
                    'active_strategies': list(self.strategies.keys()),
                    'strategy_info': {
                        sid: strategy.get_strategy_info() 
                        for sid, strategy in self.strategies.items()
                    }
                }
                
                # Publish status response
                await self.rabbitmq_publisher.publish(status, 'strategy.status')
                
            else:
                logger.warning(f"Unknown command type: {command_type}")
                
        except Exception as e:
            logger.error(f"Error handling command: {e}")
    
    async def run(self):
        """Main service loop."""
        self.running = True
        
        try:
            await self.initialize()
            
            # Start consuming market data from Redis
            market_data_task = asyncio.create_task(
                self.redis_subscriber.subscribe(
                    ['market-data:*:ohlcv'], 
                    self.process_market_data
                )
            )
            
            # Start consuming commands from RabbitMQ
            command_task = asyncio.create_task(
                self.command_consumer.consume(
                    'strategy-commands',
                    self.handle_command,
                    'commands.strategy'
                )
            )
            
            logger.info("Strategy Engine started successfully")
            
            # Run both tasks
            await asyncio.gather(market_data_task, command_task, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error in main service loop: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources."""
        self.running = False
        
        try:
            await self.redis_subscriber.close()
            await self.rabbitmq_publisher.close()
            await self.command_consumer.close()
            
            if self.mongo_client:
                self.mongo_client.close()
                
            logger.info("Strategy Engine cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    """Main entry point."""
    engine = StrategyEngine()
    
    try:
        await engine.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Service error: {e}")
    finally:
        await engine.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
