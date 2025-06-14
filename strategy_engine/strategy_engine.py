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
        
        # Data buffers for each symbol
        self.data_buffers = {}
        self.buffer_size = 500  # Keep last 500 candles
        
        # Control flags
        self.running = False
        self.strategies_enabled = True
        
    async def initialize(self):
        """Initialize the strategy engine."""
        try:
            # Connect to messaging systems
            await self.redis_subscriber.connect()
            await self.rabbitmq_publisher.connect()
            await self.command_consumer.connect()
            
            # Connect to MongoDB
            self.mongo_client = MongoClient(self.mongodb_url)
            self.db = self.mongo_client['trading_data']
            
            # Load strategy configurations
            await self.load_strategy_configs()
            
            # Initialize strategies
            self.initialize_strategies()
            
            logger.info("Strategy Engine initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Strategy Engine: {e}")
            raise
    
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
        """Process incoming market data and generate signals."""
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
            
            # Initialize buffer if needed
            if buffer_key not in self.data_buffers:
                self.data_buffers[buffer_key] = []
            
            # Add new data point
            data_point = {
                'timestamp': data['timestamp'],
                'datetime': pd.to_datetime(data['datetime']),
                'open': data['open'],
                'high': data['high'],
                'low': data['low'],
                'close': data['close'],
                'volume': data['volume']
            }
            
            self.data_buffers[buffer_key].append(data_point)
            
            # Maintain buffer size
            if len(self.data_buffers[buffer_key]) > self.buffer_size:
                self.data_buffers[buffer_key] = self.data_buffers[buffer_key][-self.buffer_size:]
            
            # Only process if we have sufficient data
            if len(self.data_buffers[buffer_key]) < 50:  # Minimum data points
                return
            
            # Convert to DataFrame
            df = pd.DataFrame(self.data_buffers[buffer_key])
            df.set_index('datetime', inplace=True)
            
            # Generate signals for each strategy
            await self.generate_strategy_signals(df, symbol, exchange, timeframe)
            
        except Exception as e:
            logger.error(f"Error processing market data: {e}")
    
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
