"""
Base strategy class for all trading strategies.
"""
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, Optional
import logging

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
