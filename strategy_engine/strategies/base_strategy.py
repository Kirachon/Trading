"""
Base strategy class for all trading strategies.
"""
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, Optional
import logging
import sys
import os

# Import rolling window for optimized processing
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
try:
    from shared.rolling_window import RollingWindow
except ImportError:
    # Fallback if import fails
    RollingWindow = None

logger = logging.getLogger(__name__)

class BaseStrategy(ABC):
    """Abstract base class for trading strategies."""
    
    def __init__(self, strategy_id: str, params: Dict[str, Any]):
        """
        Initialize strategy.
        
        Args:
            strategy_id: Unique identifier for the strategy
            params: Strategy parameters
        """
        self.strategy_id = strategy_id
        self.params = params
        self.name = self.__class__.__name__
        
        # Strategy state
        self.last_signal = 0
        self.last_signal_time = None
        self.position = 0  # Current position: 1 (long), -1 (short), 0 (flat)
        
        logger.info(f"Initialized strategy {self.name} with ID {strategy_id}")
    
    @abstractmethod
    def generate_signals(self, market_data: pd.DataFrame) -> pd.Series:
        """
        Generate trading signals based on market data.
        
        Args:
            market_data: DataFrame with OHLCV columns
            
        Returns:
            Series of signals (1: buy, -1: sell, 0: hold)
        """
        pass
    
    def validate_data(self, market_data: pd.DataFrame) -> bool:
        """
        Validate market data before processing.
        
        Args:
            market_data: DataFrame to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        
        if market_data.empty:
            logger.warning(f"Empty market data for strategy {self.strategy_id}")
            return False
        
        if not all(col in market_data.columns for col in required_columns):
            logger.error(f"Missing required columns in market data for strategy {self.strategy_id}")
            return False
        
        # Check for NaN values
        if market_data[required_columns].isnull().any().any():
            logger.warning(f"NaN values found in market data for strategy {self.strategy_id}")
            return False
        
        # Check for negative prices
        price_columns = ['open', 'high', 'low', 'close']
        if (market_data[price_columns] <= 0).any().any():
            logger.error(f"Invalid prices (<=0) found in market data for strategy {self.strategy_id}")
            return False
        
        return True
    
    def calculate_position_size(self, signal: float, current_price: float, 
                              account_balance: float, risk_per_trade: float = 0.02) -> float:
        """
        Calculate position size based on risk management.
        
        Args:
            signal: Trading signal strength
            current_price: Current market price
            account_balance: Available account balance
            risk_per_trade: Risk percentage per trade
            
        Returns:
            Position size in base currency
        """
        if signal == 0:
            return 0.0
        
        # Simple fixed percentage risk model
        risk_amount = account_balance * risk_per_trade
        position_size = risk_amount / current_price
        
        return abs(position_size) if signal != 0 else 0.0
    
    def get_signal_strength(self, signals: pd.Series) -> float:
        """
        Calculate signal strength based on recent signals.
        
        Args:
            signals: Series of recent signals
            
        Returns:
            Signal strength between -1 and 1
        """
        if len(signals) == 0:
            return 0.0
        
        # Simple moving average of recent signals
        recent_signals = signals.tail(5)  # Last 5 signals
        return recent_signals.mean()
    
    def should_generate_signal(self, current_signal: float, min_signal_interval: int = 5) -> bool:
        """
        Determine if a new signal should be generated based on timing and strength.
        
        Args:
            current_signal: Current signal value
            min_signal_interval: Minimum minutes between signals
            
        Returns:
            True if signal should be generated
        """
        # Always allow signal if no previous signal
        if self.last_signal_time is None:
            return True
        
        # Check time interval
        import datetime
        now = datetime.datetime.now()
        time_diff = (now - self.last_signal_time).total_seconds() / 60
        
        if time_diff < min_signal_interval:
            return False
        
        # Check signal change
        if abs(current_signal - self.last_signal) < 0.1:  # Minimum signal change
            return False
        
        return True
    
    def update_position(self, signal: float):
        """
        Update internal position tracking.
        
        Args:
            signal: New signal value
        """
        if signal > 0.5:
            self.position = 1  # Long
        elif signal < -0.5:
            self.position = -1  # Short
        else:
            self.position = 0  # Flat
        
        self.last_signal = signal
        import datetime
        self.last_signal_time = datetime.datetime.now()
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """
        Get strategy information and current state.
        
        Returns:
            Dictionary with strategy information
        """
        return {
            'strategy_id': self.strategy_id,
            'name': self.name,
            'parameters': self.params,
            'last_signal': self.last_signal,
            'last_signal_time': self.last_signal_time.isoformat() if self.last_signal_time else None,
            'current_position': self.position
        }
    
    def calculate_stop_loss_take_profit(self, entry_price: float, signal: float, 
                                      atr: float = None) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculate stop loss and take profit levels.
        
        Args:
            entry_price: Entry price for the trade
            signal: Trading signal (1 for long, -1 for short)
            atr: Average True Range for dynamic levels
            
        Returns:
            Tuple of (stop_loss, take_profit) prices
        """
        if signal == 0:
            return None, None
        
        # Default risk/reward ratio
        risk_reward_ratio = self.params.get('risk_reward_ratio', 2.0)
        stop_loss_pct = self.params.get('stop_loss_pct', 0.02)  # 2% default
        
        if atr and entry_price:
            # Use ATR-based stops
            atr_multiplier = self.params.get('atr_multiplier', 2.0)
            stop_distance = atr * atr_multiplier
        else:
            # Use percentage-based stops
            stop_distance = entry_price * stop_loss_pct
        
        if signal > 0:  # Long position
            stop_loss = entry_price - stop_distance
            take_profit = entry_price + (stop_distance * risk_reward_ratio)
        else:  # Short position
            stop_loss = entry_price + stop_distance
            take_profit = entry_price - (stop_distance * risk_reward_ratio)
        
        return stop_loss, take_profit

    def generate_signals_optimized(self, rolling_window) -> Optional[float]:
        """
        Generate trading signals using optimized rolling window data.

        This method can be overridden by strategies that want to use
        optimized processing. Default implementation falls back to
        DataFrame-based processing.

        Args:
            rolling_window: RollingWindow instance with OHLCV data

        Returns:
            Single signal value (1: buy, -1: sell, 0: hold) or None
        """
        if RollingWindow is None or rolling_window is None:
            return None

        # Default implementation: convert to DataFrame and use standard method
        try:
            df = rolling_window.to_dataframe()
            if df.empty:
                return None

            signals = self.generate_signals(df)
            if signals is None or signals.empty:
                return None

            return signals.iloc[-1]

        except Exception as e:
            logger.error(f"Error in optimized signal generation for {self.strategy_id}: {e}")
            return None

    def calculate_exits_optimized(self, rolling_window, signal: float) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculate stop loss and take profit using optimized rolling window data.

        Args:
            rolling_window: RollingWindow instance with OHLCV data
            signal: Trading signal value

        Returns:
            Tuple of (stop_loss, take_profit) prices
        """
        if RollingWindow is None or rolling_window is None:
            return None, None

        try:
            latest_point = rolling_window.get_latest()
            if not latest_point:
                return None, None

            entry_price = latest_point.close

            # Calculate ATR for dynamic stops (simplified)
            if len(rolling_window) >= 14:
                highs = rolling_window.get_highs(14)
                lows = rolling_window.get_lows(14)
                closes = rolling_window.get_closes(14)

                # Simple ATR calculation
                high_low = highs - lows
                high_close = np.abs(highs[1:] - closes[:-1])
                low_close = np.abs(lows[1:] - closes[:-1])

                true_ranges = np.maximum(high_low[1:], np.maximum(high_close, low_close))
                atr = np.mean(true_ranges) if len(true_ranges) > 0 else None
            else:
                atr = None

            return self.calculate_stop_loss_take_profit(entry_price, signal, atr)

        except Exception as e:
            logger.error(f"Error calculating optimized exits for {self.strategy_id}: {e}")
            return None, None
