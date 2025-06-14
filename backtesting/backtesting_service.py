"""
Backtesting Service
Comprehensive backtesting with vectorbt and backtesting.py integration.
"""
import asyncio
import logging
import os
import sys
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
import numpy as np
import vectorbt as vbt
from backtesting import Backtest, Strategy
import pymongo
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import RealDictCursor
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import seaborn as sns
from scipy import stats
from sklearn.model_selection import ParameterGrid

# Add shared modules to path
sys.path.append('/app')
from shared.messaging import MessageConsumer, RabbitMQPublisher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BacktestingService:
    """Comprehensive backtesting service with vectorbt and backtesting.py."""

    def __init__(self):
        self.mongodb_url = os.getenv('MONGODB_URL', 'mongodb://admin:SecureDB2024!@mongodb:27017/trading_data?authSource=admin')
        self.postgres_url = os.getenv('POSTGRES_URL', 'postgresql://admin:SecureDB2024!@postgresql:5432/trading')
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:SecureRabbit2024!@rabbitmq:5672/')

        # Messaging components
        self.command_consumer = MessageConsumer(self.rabbitmq_url)
        self.rabbitmq_publisher = RabbitMQPublisher(self.rabbitmq_url)

        # Database connections
        self.mongo_client = None
        self.postgres_conn = None
        self.db = None

        # Backtesting results storage
        self.results_collection = None

        # Control flags
        self.running = False

        # Strategy implementations for backtesting
        self.strategy_implementations = {}

    async def initialize(self):
        """Initialize the backtesting service."""
        try:
            # Connect to messaging systems
            await self.command_consumer.connect()
            await self.rabbitmq_publisher.connect()

            # Connect to databases
            self.mongo_client = MongoClient(self.mongodb_url)
            self.db = self.mongo_client['trading_data']
            self.results_collection = self.db['backtest_results']

            self.postgres_conn = psycopg2.connect(self.postgres_url)

            # Create indexes for backtest results
            self.results_collection.create_index([("backtest_id", 1)])
            self.results_collection.create_index([("strategy_id", 1)])
            self.results_collection.create_index([("symbol", 1)])
            self.results_collection.create_index([("created_at", 1)])

            # Initialize strategy implementations
            self.initialize_strategy_implementations()

            logger.info("Backtesting Service initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Backtesting Service: {e}")
            raise

    def initialize_strategy_implementations(self):
        """Initialize strategy implementations for backtesting."""
        # Import strategy classes
        sys.path.append('/app/../strategy_engine')
        from strategies.moving_average import MovingAverageCrossover
        from strategies.rsi import RSIStrategy
        from strategies.macd import MACDStrategy
        from strategies.bollinger_bands import BollingerBandsStrategy

        self.strategy_implementations = {
            'ma_crossover': MovingAverageCrossover,
            'rsi_strategy': RSIStrategy,
            'macd_strategy': MACDStrategy,
            'bollinger_bands': BollingerBandsStrategy
        }

    async def get_historical_data(self, symbol: str, timeframe: str,
                                start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get historical data for backtesting."""
        try:
            collection_name = f"binance_{symbol.replace('/', '_')}_{timeframe}"
            collection = self.db[collection_name]

            # Query data within date range
            start_timestamp = int(start_date.timestamp() * 1000)
            end_timestamp = int(end_date.timestamp() * 1000)

            cursor = collection.find({
                'timestamp': {
                    '$gte': start_timestamp,
                    '$lte': end_timestamp
                }
            }).sort('timestamp', 1)

            data = list(cursor)

            if not data:
                logger.warning(f"No historical data found for {symbol} {timeframe}")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(data)
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('datetime', inplace=True)

            # Ensure required columns
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in required_columns:
                if col not in df.columns:
                    logger.error(f"Missing required column: {col}")
                    return pd.DataFrame()

            return df[required_columns].astype(float)

        except Exception as e:
            logger.error(f"Error getting historical data: {e}")
            return pd.DataFrame()

    async def run_vectorbt_backtest(self, strategy_id: str, symbol: str,
                                  timeframe: str, start_date: datetime,
                                  end_date: datetime, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Run vectorized backtest using vectorbt."""
        try:
            logger.info(f"Running vectorbt backtest: {strategy_id} on {symbol}")

            # Get historical data
            data = await self.get_historical_data(symbol, timeframe, start_date, end_date)

            if data.empty:
                return {'error': 'No historical data available'}

            # Get strategy implementation
            if strategy_id not in self.strategy_implementations:
                return {'error': f'Strategy {strategy_id} not implemented for backtesting'}

            strategy_class = self.strategy_implementations[strategy_id]
            strategy = strategy_class(strategy_id, parameters)

            # Generate signals
            signals = strategy.generate_signals(data)

            if signals.empty or signals.sum() == 0:
                return {'error': 'No signals generated'}

            # Convert signals to entries and exits
            entries = signals == 1.0
            exits = signals == -1.0

            # Run vectorbt portfolio simulation
            portfolio = vbt.Portfolio.from_signals(
                data['close'],
                entries,
                exits,
                init_cash=100000,  # $100k initial capital
                fees=0.001,        # 0.1% fees
                freq='1D'
            )

            # Calculate performance metrics
            stats = portfolio.stats()

            # Create results dictionary
            results = {
                'backtest_id': str(uuid.uuid4()),
                'strategy_id': strategy_id,
                'symbol': symbol,
                'timeframe': timeframe,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'parameters': parameters,
                'backtest_type': 'vectorbt',
                'performance_metrics': {
                    'total_return': float(stats['Total Return [%]']),
                    'annual_return': float(stats['Annual Return [%]']),
                    'volatility': float(stats['Volatility [%]']),
                    'sharpe_ratio': float(stats['Sharpe Ratio']),
                    'max_drawdown': float(stats['Max Drawdown [%]']),
                    'total_trades': int(portfolio.trades.count()),
                    'win_rate': float(portfolio.trades.win_rate),
                    'profit_factor': float(portfolio.trades.profit_factor),
                    'avg_trade_duration': str(portfolio.trades.duration.mean()),
                    'final_value': float(portfolio.value().iloc[-1])
                },
                'equity_curve': portfolio.value().to_dict(),
                'trades': portfolio.trades.records_readable.to_dict('records'),
                'created_at': datetime.now(),
                'status': 'completed'
            }

            # Store results
            self.results_collection.insert_one(results)

            logger.info(f"Vectorbt backtest completed: {results['backtest_id']}")
            return results

        except Exception as e:
            logger.error(f"Error in vectorbt backtest: {e}")
            return {'error': str(e)}

    async def run_event_driven_backtest(self, strategy_id: str, symbol: str,
                                      timeframe: str, start_date: datetime,
                                      end_date: datetime, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Run event-driven backtest using backtesting.py."""
        try:
            logger.info(f"Running event-driven backtest: {strategy_id} on {symbol}")

            # Get historical data
            data = await self.get_historical_data(symbol, timeframe, start_date, end_date)

            if data.empty:
                return {'error': 'No historical data available'}

            # Create backtesting.py strategy wrapper
            strategy_wrapper = self.create_backtesting_strategy(strategy_id, parameters)

            # Run backtest
            bt = Backtest(
                data,
                strategy_wrapper,
                cash=100000,
                commission=0.001,
                exclusive_orders=True
            )

            result = bt.run()

            # Extract performance metrics
            results = {
                'backtest_id': str(uuid.uuid4()),
                'strategy_id': strategy_id,
                'symbol': symbol,
                'timeframe': timeframe,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'parameters': parameters,
                'backtest_type': 'event_driven',
                'performance_metrics': {
                    'total_return': float(result['Return [%]']),
                    'annual_return': float(result['Return (Ann.) [%]']),
                    'volatility': float(result['Volatility (Ann.) [%]']),
                    'sharpe_ratio': float(result['Sharpe Ratio']),
                    'max_drawdown': float(result['Max. Drawdown [%]']),
                    'total_trades': int(result['# Trades']),
                    'win_rate': float(result['Win Rate [%]']),
                    'profit_factor': float(result['Profit Factor']),
                    'avg_trade_duration': str(result['Avg. Trade Duration']),
                    'final_value': float(result['Equity Final [$]'])
                },
                'equity_curve': result._equity_curve.to_dict(),
                'trades': result._trades.to_dict('records') if hasattr(result, '_trades') else [],
                'created_at': datetime.now(),
                'status': 'completed'
            }

            # Store results
            self.results_collection.insert_one(results)

            logger.info(f"Event-driven backtest completed: {results['backtest_id']}")
            return results

        except Exception as e:
            logger.error(f"Error in event-driven backtest: {e}")
            return {'error': str(e)}

    def create_backtesting_strategy(self, strategy_id: str, parameters: Dict[str, Any]):
        """Create backtesting.py strategy wrapper."""

        class StrategyWrapper(Strategy):
            def init(self):
                # Initialize strategy with parameters
                self.strategy_id = strategy_id
                self.parameters = parameters

                # Import and initialize the actual strategy
                if strategy_id in self.strategy_implementations:
                    strategy_class = self.strategy_implementations[strategy_id]
                    self.strategy = strategy_class(strategy_id, parameters)
                else:
                    raise ValueError(f"Strategy {strategy_id} not found")

            def next(self):
                # Get current data window
                current_data = pd.DataFrame({
                    'open': self.data.Open,
                    'high': self.data.High,
                    'low': self.data.Low,
                    'close': self.data.Close,
                    'volume': self.data.Volume
                })

                # Generate signals
                try:
                    signals = self.strategy.generate_signals(current_data)

                    if not signals.empty:
                        current_signal = signals.iloc[-1]

                        if current_signal > 0.5 and not self.position:
                            self.buy()
                        elif current_signal < -0.5 and self.position:
                            self.sell()

                except Exception as e:
                    logger.error(f"Error in strategy next(): {e}")

        return StrategyWrapper

    async def run_parameter_optimization(self, strategy_id: str, symbol: str,
                                       timeframe: str, start_date: datetime,
                                       end_date: datetime, param_grid: Dict[str, List]) -> Dict[str, Any]:
        """Run parameter optimization using grid search."""
        try:
            logger.info(f"Running parameter optimization: {strategy_id} on {symbol}")

            # Generate parameter combinations
            param_combinations = list(ParameterGrid(param_grid))

            if len(param_combinations) > 100:
                logger.warning(f"Large parameter grid ({len(param_combinations)} combinations), limiting to 100")
                param_combinations = param_combinations[:100]

            optimization_results = []

            for i, params in enumerate(param_combinations):
                logger.info(f"Testing parameter set {i+1}/{len(param_combinations)}: {params}")

                # Run backtest with these parameters
                result = await self.run_vectorbt_backtest(
                    strategy_id, symbol, timeframe, start_date, end_date, params
                )

                if 'error' not in result:
                    optimization_results.append({
                        'parameters': params,
                        'performance': result['performance_metrics'],
                        'backtest_id': result['backtest_id']
                    })

            if not optimization_results:
                return {'error': 'No successful backtests in optimization'}

            # Find best parameters based on Sharpe ratio
            best_result = max(optimization_results, key=lambda x: x['performance']['sharpe_ratio'])

            # Calculate optimization statistics
            sharpe_ratios = [r['performance']['sharpe_ratio'] for r in optimization_results]
            returns = [r['performance']['total_return'] for r in optimization_results]

            optimization_summary = {
                'optimization_id': str(uuid.uuid4()),
                'strategy_id': strategy_id,
                'symbol': symbol,
                'timeframe': timeframe,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'parameter_grid': param_grid,
                'total_combinations': len(param_combinations),
                'successful_backtests': len(optimization_results),
                'best_parameters': best_result['parameters'],
                'best_performance': best_result['performance'],
                'optimization_stats': {
                    'mean_sharpe': np.mean(sharpe_ratios),
                    'std_sharpe': np.std(sharpe_ratios),
                    'mean_return': np.mean(returns),
                    'std_return': np.std(returns),
                    'best_sharpe': max(sharpe_ratios),
                    'worst_sharpe': min(sharpe_ratios)
                },
                'all_results': optimization_results,
                'created_at': datetime.now(),
                'status': 'completed'
            }

            # Store optimization results
            self.db['optimization_results'].insert_one(optimization_summary)

            logger.info(f"Parameter optimization completed: {optimization_summary['optimization_id']}")
            return optimization_summary

        except Exception as e:
            logger.error(f"Error in parameter optimization: {e}")
            return {'error': str(e)}

    async def run_monte_carlo_simulation(self, strategy_id: str, symbol: str,
                                       timeframe: str, start_date: datetime,
                                       end_date: datetime, parameters: Dict[str, Any],
                                       num_simulations: int = 1000) -> Dict[str, Any]:
        """Run Monte Carlo simulation for risk analysis."""
        try:
            logger.info(f"Running Monte Carlo simulation: {strategy_id} on {symbol}")

            # Get historical data
            data = await self.get_historical_data(symbol, timeframe, start_date, end_date)

            if data.empty:
                return {'error': 'No historical data available'}

            # Calculate returns
            returns = data['close'].pct_change().dropna()

            if len(returns) < 100:
                return {'error': 'Insufficient data for Monte Carlo simulation'}

            # Fit distribution to returns
            mu = returns.mean()
            sigma = returns.std()

            simulation_results = []

            for sim in range(num_simulations):
                # Generate random returns
                random_returns = np.random.normal(mu, sigma, len(returns))

                # Create synthetic price series
                synthetic_prices = [data['close'].iloc[0]]
                for ret in random_returns:
                    synthetic_prices.append(synthetic_prices[-1] * (1 + ret))

                # Create synthetic OHLCV data (simplified)
                synthetic_data = data.copy()
                synthetic_data['close'] = synthetic_prices[1:]
                synthetic_data['open'] = synthetic_data['close'].shift(1)
                synthetic_data['high'] = synthetic_data[['open', 'close']].max(axis=1) * (1 + np.random.uniform(0, 0.02, len(synthetic_data)))
                synthetic_data['low'] = synthetic_data[['open', 'close']].min(axis=1) * (1 - np.random.uniform(0, 0.02, len(synthetic_data)))
                synthetic_data = synthetic_data.dropna()

                # Run strategy on synthetic data
                if strategy_id in self.strategy_implementations:
                    strategy_class = self.strategy_implementations[strategy_id]
                    strategy = strategy_class(strategy_id, parameters)

                    try:
                        signals = strategy.generate_signals(synthetic_data)

                        if not signals.empty and signals.sum() != 0:
                            # Simple portfolio simulation
                            portfolio_value = 100000  # Starting value
                            position = 0
                            trades = []

                            for i in range(len(signals)):
                                signal = signals.iloc[i]
                                price = synthetic_data['close'].iloc[i]

                                if signal > 0.5 and position == 0:  # Buy
                                    position = portfolio_value / price
                                    portfolio_value = 0
                                    trades.append({'type': 'buy', 'price': price, 'value': position * price})

                                elif signal < -0.5 and position > 0:  # Sell
                                    portfolio_value = position * price
                                    position = 0
                                    trades.append({'type': 'sell', 'price': price, 'value': portfolio_value})

                            # Final portfolio value
                            if position > 0:
                                portfolio_value = position * synthetic_data['close'].iloc[-1]

                            final_return = (portfolio_value - 100000) / 100000 * 100
                            simulation_results.append({
                                'simulation': sim,
                                'final_value': portfolio_value,
                                'total_return': final_return,
                                'num_trades': len(trades)
                            })

                    except Exception as e:
                        logger.debug(f"Error in simulation {sim}: {e}")
                        continue

            if not simulation_results:
                return {'error': 'No successful simulations'}

            # Calculate statistics
            returns_list = [r['total_return'] for r in simulation_results]
            values_list = [r['final_value'] for r in simulation_results]

            # Calculate VaR and CVaR
            var_95 = np.percentile(returns_list, 5)
            var_99 = np.percentile(returns_list, 1)
            cvar_95 = np.mean([r for r in returns_list if r <= var_95])
            cvar_99 = np.mean([r for r in returns_list if r <= var_99])

            monte_carlo_results = {
                'simulation_id': str(uuid.uuid4()),
                'strategy_id': strategy_id,
                'symbol': symbol,
                'timeframe': timeframe,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'parameters': parameters,
                'num_simulations': num_simulations,
                'successful_simulations': len(simulation_results),
                'statistics': {
                    'mean_return': np.mean(returns_list),
                    'std_return': np.std(returns_list),
                    'min_return': np.min(returns_list),
                    'max_return': np.max(returns_list),
                    'median_return': np.median(returns_list),
                    'var_95': var_95,
                    'var_99': var_99,
                    'cvar_95': cvar_95,
                    'cvar_99': cvar_99,
                    'probability_of_loss': len([r for r in returns_list if r < 0]) / len(returns_list),
                    'probability_of_profit': len([r for r in returns_list if r > 0]) / len(returns_list)
                },
                'return_distribution': returns_list,
                'value_distribution': values_list,
                'detailed_results': simulation_results,
                'created_at': datetime.now(),
                'status': 'completed'
            }

            # Store Monte Carlo results
            self.db['monte_carlo_results'].insert_one(monte_carlo_results)

            logger.info(f"Monte Carlo simulation completed: {monte_carlo_results['simulation_id']}")
            return monte_carlo_results

        except Exception as e:
            logger.error(f"Error in Monte Carlo simulation: {e}")
            return {'error': str(e)}

    async def run_walk_forward_analysis(self, strategy_id: str, symbol: str,
                                      timeframe: str, start_date: datetime,
                                      end_date: datetime, parameters: Dict[str, Any],
                                      train_period_days: int = 252,
                                      test_period_days: int = 63) -> Dict[str, Any]:
        """Run walk-forward analysis."""
        try:
            logger.info(f"Running walk-forward analysis: {strategy_id} on {symbol}")

            current_date = start_date
            walk_forward_results = []

            while current_date + timedelta(days=train_period_days + test_period_days) <= end_date:
                train_start = current_date
                train_end = current_date + timedelta(days=train_period_days)
                test_start = train_end
                test_end = train_end + timedelta(days=test_period_days)

                logger.info(f"Walk-forward period: train {train_start.date()} to {train_end.date()}, test {test_start.date()} to {test_end.date()}")

                # Run backtest on test period
                test_result = await self.run_vectorbt_backtest(
                    strategy_id, symbol, timeframe, test_start, test_end, parameters
                )

                if 'error' not in test_result:
                    walk_forward_results.append({
                        'train_start': train_start.isoformat(),
                        'train_end': train_end.isoformat(),
                        'test_start': test_start.isoformat(),
                        'test_end': test_end.isoformat(),
                        'performance': test_result['performance_metrics'],
                        'backtest_id': test_result['backtest_id']
                    })

                # Move to next period
                current_date += timedelta(days=test_period_days)

            if not walk_forward_results:
                return {'error': 'No successful walk-forward periods'}

            # Calculate aggregate statistics
            returns = [r['performance']['total_return'] for r in walk_forward_results]
            sharpe_ratios = [r['performance']['sharpe_ratio'] for r in walk_forward_results]

            walk_forward_summary = {
                'walk_forward_id': str(uuid.uuid4()),
                'strategy_id': strategy_id,
                'symbol': symbol,
                'timeframe': timeframe,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'parameters': parameters,
                'train_period_days': train_period_days,
                'test_period_days': test_period_days,
                'num_periods': len(walk_forward_results),
                'aggregate_stats': {
                    'mean_return': np.mean(returns),
                    'std_return': np.std(returns),
                    'mean_sharpe': np.mean(sharpe_ratios),
                    'std_sharpe': np.std(sharpe_ratios),
                    'consistency_ratio': len([r for r in returns if r > 0]) / len(returns),
                    'worst_period_return': min(returns),
                    'best_period_return': max(returns)
                },
                'period_results': walk_forward_results,
                'created_at': datetime.now(),
                'status': 'completed'
            }

            # Store walk-forward results
            self.db['walk_forward_results'].insert_one(walk_forward_summary)

            logger.info(f"Walk-forward analysis completed: {walk_forward_summary['walk_forward_id']}")
            return walk_forward_summary

        except Exception as e:
            logger.error(f"Error in walk-forward analysis: {e}")
            return {'error': str(e)}

    async def handle_command(self, command: Dict[str, Any]):
        """Handle backtesting commands."""
        try:
            command_type = command.get('type')

            if command_type == 'run_backtest':
                strategy_id = command.get('strategy_id')
                symbol = command.get('symbol')
                timeframe = command.get('timeframe', '1h')
                start_date = datetime.fromisoformat(command.get('start_date'))
                end_date = datetime.fromisoformat(command.get('end_date'))
                parameters = command.get('parameters', {})
                backtest_type = command.get('backtest_type', 'vectorbt')

                if backtest_type == 'vectorbt':
                    result = await self.run_vectorbt_backtest(
                        strategy_id, symbol, timeframe, start_date, end_date, parameters
                    )
                elif backtest_type == 'event_driven':
                    result = await self.run_event_driven_backtest(
                        strategy_id, symbol, timeframe, start_date, end_date, parameters
                    )
                else:
                    result = {'error': f'Unknown backtest type: {backtest_type}'}

                await self.rabbitmq_publisher.publish(result, 'backtesting.results')

            elif command_type == 'run_optimization':
                strategy_id = command.get('strategy_id')
                symbol = command.get('symbol')
                timeframe = command.get('timeframe', '1h')
                start_date = datetime.fromisoformat(command.get('start_date'))
                end_date = datetime.fromisoformat(command.get('end_date'))
                param_grid = command.get('param_grid', {})

                result = await self.run_parameter_optimization(
                    strategy_id, symbol, timeframe, start_date, end_date, param_grid
                )

                await self.rabbitmq_publisher.publish(result, 'backtesting.optimization_results')

            elif command_type == 'run_monte_carlo':
                strategy_id = command.get('strategy_id')
                symbol = command.get('symbol')
                timeframe = command.get('timeframe', '1h')
                start_date = datetime.fromisoformat(command.get('start_date'))
                end_date = datetime.fromisoformat(command.get('end_date'))
                parameters = command.get('parameters', {})
                num_simulations = command.get('num_simulations', 1000)

                result = await self.run_monte_carlo_simulation(
                    strategy_id, symbol, timeframe, start_date, end_date,
                    parameters, num_simulations
                )

                await self.rabbitmq_publisher.publish(result, 'backtesting.monte_carlo_results')

            elif command_type == 'run_walk_forward':
                strategy_id = command.get('strategy_id')
                symbol = command.get('symbol')
                timeframe = command.get('timeframe', '1h')
                start_date = datetime.fromisoformat(command.get('start_date'))
                end_date = datetime.fromisoformat(command.get('end_date'))
                parameters = command.get('parameters', {})
                train_period = command.get('train_period_days', 252)
                test_period = command.get('test_period_days', 63)

                result = await self.run_walk_forward_analysis(
                    strategy_id, symbol, timeframe, start_date, end_date,
                    parameters, train_period, test_period
                )

                await self.rabbitmq_publisher.publish(result, 'backtesting.walk_forward_results')

            elif command_type == 'get_backtest_results':
                backtest_id = command.get('backtest_id')
                strategy_id = command.get('strategy_id')

                query = {}
                if backtest_id:
                    query['backtest_id'] = backtest_id
                elif strategy_id:
                    query['strategy_id'] = strategy_id

                results = list(self.results_collection.find(query).sort('created_at', -1).limit(50))

                # Convert ObjectId to string for JSON serialization
                for result in results:
                    result['_id'] = str(result['_id'])
                    if 'created_at' in result:
                        result['created_at'] = result['created_at'].isoformat()

                await self.rabbitmq_publisher.publish({'results': results}, 'backtesting.query_results')

            else:
                logger.warning(f"Unknown command type: {command_type}")

        except Exception as e:
            logger.error(f"Error handling command: {e}")
            error_response = {'error': str(e), 'command': command}
            await self.rabbitmq_publisher.publish(error_response, 'backtesting.errors')

    async def run(self):
        """Main service loop."""
        self.running = True

        try:
            await self.initialize()

            # Start consuming commands
            command_task = asyncio.create_task(
                self.command_consumer.consume(
                    'backtesting-commands',
                    self.handle_command,
                    'commands.backtesting'
                )
            )

            logger.info("Backtesting Service started successfully")

            # Run command task
            await command_task

        except Exception as e:
            logger.error(f"Error in main service loop: {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Cleanup resources."""
        self.running = False

        try:
            await self.command_consumer.close()
            await self.rabbitmq_publisher.close()

            if self.mongo_client:
                self.mongo_client.close()

            if self.postgres_conn:
                self.postgres_conn.close()

            logger.info("Backtesting Service cleanup completed")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    """Main entry point."""
    service = BacktestingService()

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