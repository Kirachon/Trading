"""
Pytest configuration and fixtures for the trading system tests.
"""
import pytest
import asyncio
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
import pandas as pd
import numpy as np
import fakeredis.aioredis
import mongomock

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def mock_redis():
    """Mock Redis client for testing."""
    return fakeredis.aioredis.FakeRedis()

@pytest.fixture
def mock_mongo():
    """Mock MongoDB client for testing."""
    return mongomock.MongoClient()

@pytest.fixture
def mock_postgres():
    """Mock PostgreSQL connection for testing."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn

@pytest.fixture
def sample_market_data():
    """Generate sample market data for testing."""
    dates = pd.date_range(start='2023-01-01', end='2023-12-31', freq='1H')
    np.random.seed(42)  # For reproducible tests
    
    # Generate realistic OHLCV data
    base_price = 50000  # Starting price
    returns = np.random.normal(0, 0.02, len(dates))  # 2% volatility
    
    prices = [base_price]
    for ret in returns[1:]:
        prices.append(prices[-1] * (1 + ret))
    
    # Create OHLCV data
    data = []
    for i, (date, price) in enumerate(zip(dates, prices)):
        high = price * (1 + abs(np.random.normal(0, 0.01)))
        low = price * (1 - abs(np.random.normal(0, 0.01)))
        open_price = prices[i-1] if i > 0 else price
        close_price = price
        volume = np.random.uniform(100, 1000)
        
        data.append({
            'timestamp': int(date.timestamp() * 1000),
            'datetime': date,
            'open': open_price,
            'high': max(open_price, high, close_price),
            'low': min(open_price, low, close_price),
            'close': close_price,
            'volume': volume
        })
    
    df = pd.DataFrame(data)
    df.set_index('datetime', inplace=True)
    return df

@pytest.fixture
def sample_strategy_config():
    """Sample strategy configuration for testing."""
    return {
        'ma_crossover': {
            'short_window': 10,
            'long_window': 20,
            'ma_type': 'sma'
        },
        'rsi_strategy': {
            'rsi_period': 14,
            'oversold': 30,
            'overbought': 70
        }
    }

@pytest.fixture
def sample_trade_data():
    """Sample trade data for testing."""
    return [
        {
            'id': 1,
            'exchange_id': 'binance',
            'symbol': 'BTC/USDT',
            'order_id': 'order_123',
            'side': 'buy',
            'quantity': 0.1,
            'price': 50000.0,
            'timestamp': int(datetime.now().timestamp() * 1000),
            'strategy_id': 'ma_crossover',
            'status': 'filled',
            'commission': 5.0,
            'realized_pnl': 0.0,
            'created_at': datetime.now()
        },
        {
            'id': 2,
            'exchange_id': 'binance',
            'symbol': 'BTC/USDT',
            'order_id': 'order_124',
            'side': 'sell',
            'quantity': 0.1,
            'price': 51000.0,
            'timestamp': int(datetime.now().timestamp() * 1000),
            'strategy_id': 'ma_crossover',
            'status': 'filled',
            'commission': 5.1,
            'realized_pnl': 990.0,
            'created_at': datetime.now()
        }
    ]

@pytest.fixture
def mock_exchange():
    """Mock exchange for testing."""
    exchange = MagicMock()
    exchange.id = 'binance'
    exchange.name = 'Binance'
    exchange.has = {
        'fetchOHLCV': True,
        'fetchTicker': True,
        'createOrder': True,
        'fetchOrder': True,
        'fetchBalance': True
    }
    
    # Mock methods
    exchange.fetch_ohlcv = AsyncMock(return_value=[
        [1640995200000, 50000, 51000, 49000, 50500, 100],  # Sample OHLCV
        [1640998800000, 50500, 51500, 50000, 51000, 150]
    ])
    
    exchange.fetch_ticker = AsyncMock(return_value={
        'symbol': 'BTC/USDT',
        'last': 50000,
        'bid': 49950,
        'ask': 50050,
        'volume': 1000
    })
    
    exchange.create_market_order = AsyncMock(return_value={
        'id': 'order_123',
        'symbol': 'BTC/USDT',
        'side': 'buy',
        'amount': 0.1,
        'price': 50000,
        'status': 'closed',
        'filled': 0.1
    })
    
    exchange.fetch_balance = AsyncMock(return_value={
        'USDT': {'free': 10000, 'used': 0, 'total': 10000},
        'BTC': {'free': 0.5, 'used': 0, 'total': 0.5}
    })
    
    return exchange

@pytest.fixture
def mock_rabbitmq_publisher():
    """Mock RabbitMQ publisher for testing."""
    publisher = AsyncMock()
    publisher.connect = AsyncMock()
    publisher.publish = AsyncMock()
    publisher.close = AsyncMock()
    return publisher

@pytest.fixture
def mock_rabbitmq_consumer():
    """Mock RabbitMQ consumer for testing."""
    consumer = AsyncMock()
    consumer.connect = AsyncMock()
    consumer.consume = AsyncMock()
    consumer.close = AsyncMock()
    return consumer

@pytest.fixture
def sample_portfolio_data():
    """Sample portfolio data for testing."""
    return {
        'positions': [
            {
                'id': 1,
                'exchange_id': 'binance',
                'symbol': 'BTC/USDT',
                'side': 'long',
                'quantity': 0.1,
                'avg_price': 50000.0,
                'unrealized_pnl': 100.0,
                'realized_pnl': 0.0,
                'strategy_id': 'ma_crossover',
                'opened_at': datetime.now() - timedelta(hours=2)
            }
        ],
        'balances': [
            {
                'exchange_id': 'binance',
                'asset': 'USDT',
                'balance': 9500.0,
                'locked': 0.0,
                'updated_at': datetime.now()
            },
            {
                'exchange_id': 'binance',
                'asset': 'BTC',
                'balance': 0.1,
                'locked': 0.0,
                'updated_at': datetime.now()
            }
        ]
    }

@pytest.fixture
def sample_risk_limits():
    """Sample risk limits for testing."""
    return {
        'binance:BTC/USDT': {
            'max_position_size': 10000.0,
            'max_daily_loss': 1000.0,
            'max_trades_per_day': 50,
            'fat_finger_threshold': 0.05
        },
        'default': {
            'max_position_size': 5000.0,
            'max_daily_loss': 500.0,
            'max_trades_per_day': 25,
            'fat_finger_threshold': 0.03
        }
    }

@pytest.fixture
def sample_backtest_results():
    """Sample backtest results for testing."""
    return {
        'backtest_id': 'test_backtest_123',
        'strategy_id': 'ma_crossover',
        'symbol': 'BTC/USDT',
        'timeframe': '1h',
        'start_date': '2023-01-01T00:00:00',
        'end_date': '2023-12-31T23:59:59',
        'parameters': {'short_window': 10, 'long_window': 20},
        'performance_metrics': {
            'total_return': 15.5,
            'annual_return': 15.5,
            'volatility': 25.3,
            'sharpe_ratio': 0.61,
            'max_drawdown': -8.2,
            'total_trades': 45,
            'win_rate': 0.62,
            'profit_factor': 1.35,
            'final_value': 115500.0
        },
        'status': 'completed'
    }

# Test environment setup
@pytest.fixture(autouse=True)
def setup_test_environment():
    """Set up test environment variables."""
    os.environ.update({
        'REDIS_URL': 'redis://localhost:6379',
        'RABBITMQ_URL': 'amqp://localhost/',
        'POSTGRES_URL': 'postgresql://test:test@localhost:5432/test_trading',
        'MONGODB_URL': 'mongodb://localhost:27017/test_trading_data',
        'TESTING': 'true'
    })
    yield
    # Cleanup if needed

# Async test helpers
@pytest.fixture
def async_test_client():
    """Helper for async test client setup."""
    async def _create_client(service_class, *args, **kwargs):
        client = service_class(*args, **kwargs)
        await client.initialize()
        return client
    return _create_client
