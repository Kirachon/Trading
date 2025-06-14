"""
Unit tests for trading strategies.
"""
import pytest
import pandas as pd
import numpy as np
import sys
import os

# Add strategy engine to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../strategy_engine'))

from strategies.moving_average import MovingAverageCrossover
from strategies.rsi import RSIStrategy
from strategies.macd import MACDStrategy
from strategies.bollinger_bands import BollingerBandsStrategy

class TestMovingAverageCrossover:
    """Test cases for Moving Average Crossover strategy."""
    
    def test_strategy_initialization(self):
        """Test strategy initialization with parameters."""
        params = {'short_window': 10, 'long_window': 20, 'ma_type': 'sma'}
        strategy = MovingAverageCrossover('test_ma', params)
        
        assert strategy.strategy_id == 'test_ma'
        assert strategy.short_window == 10
        assert strategy.long_window == 20
        assert strategy.ma_type == 'sma'
    
    def test_invalid_parameters(self):
        """Test strategy initialization with invalid parameters."""
        # Short window >= long window should raise error
        params = {'short_window': 20, 'long_window': 10, 'ma_type': 'sma'}
        
        with pytest.raises(ValueError):
            MovingAverageCrossover('test_ma', params)
    
    def test_signal_generation(self, sample_market_data):
        """Test signal generation with sample data."""
        params = {'short_window': 5, 'long_window': 10, 'ma_type': 'sma'}
        strategy = MovingAverageCrossover('test_ma', params)
        
        signals = strategy.generate_signals(sample_market_data)
        
        assert isinstance(signals, pd.Series)
        assert len(signals) == len(sample_market_data)
        assert signals.index.equals(sample_market_data.index)
        
        # Check signal values are valid
        assert all(signals.isin([0, 1, -1]))
    
    def test_insufficient_data(self):
        """Test strategy behavior with insufficient data."""
        params = {'short_window': 10, 'long_window': 20, 'ma_type': 'sma'}
        strategy = MovingAverageCrossover('test_ma', params)
        
        # Create data with only 15 rows (less than long_window)
        short_data = pd.DataFrame({
            'open': np.random.randn(15),
            'high': np.random.randn(15),
            'low': np.random.randn(15),
            'close': np.random.randn(15),
            'volume': np.random.randn(15)
        })
        
        signals = strategy.generate_signals(short_data)
        
        # Should return all zeros for insufficient data
        assert all(signals == 0)
    
    def test_ema_calculation(self, sample_market_data):
        """Test EMA calculation."""
        params = {'short_window': 5, 'long_window': 10, 'ma_type': 'ema'}
        strategy = MovingAverageCrossover('test_ma', params)
        
        signals = strategy.generate_signals(sample_market_data)
        
        assert isinstance(signals, pd.Series)
        # EMA should produce different results than SMA
        assert not signals.isna().all()

class TestRSIStrategy:
    """Test cases for RSI strategy."""
    
    def test_strategy_initialization(self):
        """Test RSI strategy initialization."""
        params = {'rsi_period': 14, 'oversold': 30, 'overbought': 70}
        strategy = RSIStrategy('test_rsi', params)
        
        assert strategy.strategy_id == 'test_rsi'
        assert strategy.rsi_period == 14
        assert strategy.oversold == 30
        assert strategy.overbought == 70
    
    def test_invalid_thresholds(self):
        """Test RSI strategy with invalid thresholds."""
        # Oversold >= overbought should raise error
        params = {'rsi_period': 14, 'oversold': 70, 'overbought': 30}
        
        with pytest.raises(ValueError):
            RSIStrategy('test_rsi', params)
    
    def test_rsi_calculation(self, sample_market_data):
        """Test RSI calculation."""
        params = {'rsi_period': 14, 'oversold': 30, 'overbought': 70}
        strategy = RSIStrategy('test_rsi', params)
        
        rsi = strategy.calculate_rsi(sample_market_data['close'])
        
        assert isinstance(rsi, pd.Series)
        assert len(rsi) == len(sample_market_data)
        
        # RSI should be between 0 and 100
        valid_rsi = rsi.dropna()
        assert all(valid_rsi >= 0)
        assert all(valid_rsi <= 100)
    
    def test_signal_generation(self, sample_market_data):
        """Test RSI signal generation."""
        params = {'rsi_period': 14, 'oversold': 30, 'overbought': 70}
        strategy = RSIStrategy('test_rsi', params)
        
        signals = strategy.generate_signals(sample_market_data)
        
        assert isinstance(signals, pd.Series)
        assert len(signals) == len(sample_market_data)
        assert all(signals.isin([0, 1, -1]))
    
    def test_divergence_detection(self, sample_market_data):
        """Test RSI divergence detection."""
        params = {'rsi_period': 14, 'oversold': 30, 'overbought': 70}
        strategy = RSIStrategy('test_rsi', params)
        
        rsi = strategy.calculate_rsi(sample_market_data['close'])
        divergences = strategy.detect_divergence(sample_market_data['close'], rsi)
        
        assert 'bullish_divergence' in divergences
        assert 'bearish_divergence' in divergences
        assert isinstance(divergences['bullish_divergence'], pd.Series)
        assert isinstance(divergences['bearish_divergence'], pd.Series)

class TestMACDStrategy:
    """Test cases for MACD strategy."""
    
    def test_strategy_initialization(self):
        """Test MACD strategy initialization."""
        params = {'fast_period': 12, 'slow_period': 26, 'signal_period': 9}
        strategy = MACDStrategy('test_macd', params)
        
        assert strategy.strategy_id == 'test_macd'
        assert strategy.fast_period == 12
        assert strategy.slow_period == 26
        assert strategy.signal_period == 9
    
    def test_invalid_periods(self):
        """Test MACD strategy with invalid periods."""
        # Fast period >= slow period should raise error
        params = {'fast_period': 26, 'slow_period': 12, 'signal_period': 9}
        
        with pytest.raises(ValueError):
            MACDStrategy('test_macd', params)
    
    def test_macd_calculation(self, sample_market_data):
        """Test MACD calculation."""
        params = {'fast_period': 12, 'slow_period': 26, 'signal_period': 9}
        strategy = MACDStrategy('test_macd', params)
        
        macd_data = strategy.calculate_macd(sample_market_data['close'])
        
        assert 'macd' in macd_data
        assert 'signal' in macd_data
        assert 'histogram' in macd_data
        
        assert isinstance(macd_data['macd'], pd.Series)
        assert isinstance(macd_data['signal'], pd.Series)
        assert isinstance(macd_data['histogram'], pd.Series)
    
    def test_signal_generation(self, sample_market_data):
        """Test MACD signal generation."""
        params = {'fast_period': 12, 'slow_period': 26, 'signal_period': 9}
        strategy = MACDStrategy('test_macd', params)
        
        signals = strategy.generate_signals(sample_market_data)
        
        assert isinstance(signals, pd.Series)
        assert len(signals) == len(sample_market_data)
        assert all(signals.isin([0, 1, -1]))

class TestBollingerBandsStrategy:
    """Test cases for Bollinger Bands strategy."""
    
    def test_strategy_initialization(self):
        """Test Bollinger Bands strategy initialization."""
        params = {'period': 20, 'std_dev': 2.0, 'mean_reversion_mode': True}
        strategy = BollingerBandsStrategy('test_bb', params)
        
        assert strategy.strategy_id == 'test_bb'
        assert strategy.period == 20
        assert strategy.std_dev == 2.0
        assert strategy.mean_reversion_mode == True
    
    def test_bollinger_bands_calculation(self, sample_market_data):
        """Test Bollinger Bands calculation."""
        params = {'period': 20, 'std_dev': 2.0, 'mean_reversion_mode': True}
        strategy = BollingerBandsStrategy('test_bb', params)
        
        bb_data = strategy.calculate_bollinger_bands(sample_market_data['close'])
        
        assert 'upper' in bb_data
        assert 'middle' in bb_data
        assert 'lower' in bb_data
        assert 'bandwidth' in bb_data
        assert 'percent_b' in bb_data
        
        # Upper band should be above middle, middle above lower
        valid_data = bb_data['upper'].dropna()
        if not valid_data.empty:
            upper = bb_data['upper'].dropna()
            middle = bb_data['middle'].dropna()
            lower = bb_data['lower'].dropna()
            
            # Align indices for comparison
            common_idx = upper.index.intersection(middle.index).intersection(lower.index)
            if not common_idx.empty:
                assert all(upper[common_idx] >= middle[common_idx])
                assert all(middle[common_idx] >= lower[common_idx])
    
    def test_squeeze_detection(self, sample_market_data):
        """Test Bollinger Bands squeeze detection."""
        params = {'period': 20, 'std_dev': 2.0, 'squeeze_threshold': 0.1}
        strategy = BollingerBandsStrategy('test_bb', params)
        
        bb_data = strategy.calculate_bollinger_bands(sample_market_data['close'])
        squeeze = strategy.detect_squeeze(bb_data)
        
        assert isinstance(squeeze, pd.Series)
        assert squeeze.dtype == bool
    
    def test_signal_generation_mean_reversion(self, sample_market_data):
        """Test signal generation in mean reversion mode."""
        params = {'period': 20, 'std_dev': 2.0, 'mean_reversion_mode': True}
        strategy = BollingerBandsStrategy('test_bb', params)
        
        signals = strategy.generate_signals(sample_market_data)
        
        assert isinstance(signals, pd.Series)
        assert len(signals) == len(sample_market_data)
        assert all(signals.isin([0, 1, -1]))
    
    def test_signal_generation_breakout(self, sample_market_data):
        """Test signal generation in breakout mode."""
        params = {'period': 20, 'std_dev': 2.0, 'mean_reversion_mode': False}
        strategy = BollingerBandsStrategy('test_bb', params)
        
        signals = strategy.generate_signals(sample_market_data)
        
        assert isinstance(signals, pd.Series)
        assert len(signals) == len(sample_market_data)
        assert all(signals.isin([0, 1, -1]))

class TestBaseStrategyFunctionality:
    """Test base strategy functionality."""
    
    def test_data_validation(self, sample_market_data):
        """Test market data validation."""
        params = {'short_window': 10, 'long_window': 20, 'ma_type': 'sma'}
        strategy = MovingAverageCrossover('test_ma', params)
        
        # Valid data should pass
        assert strategy.validate_data(sample_market_data) == True
        
        # Empty data should fail
        empty_data = pd.DataFrame()
        assert strategy.validate_data(empty_data) == False
        
        # Data missing required columns should fail
        incomplete_data = sample_market_data.drop('close', axis=1)
        assert strategy.validate_data(incomplete_data) == False
    
    def test_strategy_info(self):
        """Test strategy info retrieval."""
        params = {'short_window': 10, 'long_window': 20, 'ma_type': 'sma'}
        strategy = MovingAverageCrossover('test_ma', params)
        
        info = strategy.get_strategy_info()
        
        assert 'strategy_id' in info
        assert 'name' in info
        assert 'parameters' in info
        assert 'current_position' in info
        assert info['strategy_id'] == 'test_ma'
    
    def test_position_tracking(self):
        """Test position tracking functionality."""
        params = {'short_window': 10, 'long_window': 20, 'ma_type': 'sma'}
        strategy = MovingAverageCrossover('test_ma', params)
        
        # Initial position should be flat
        assert strategy.current_position == 0
        
        # Update position
        strategy.update_position(1.0)
        assert strategy.current_position == 1.0
        
        strategy.update_position(-1.0)
        assert strategy.current_position == -1.0
        
        strategy.update_position(0.0)
        assert strategy.current_position == 0.0
