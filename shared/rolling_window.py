"""
Efficient Rolling Window Data Structure for OHLCV Data

This module provides optimized data structures for handling streaming market data
with efficient rolling window calculations and incremental indicator updates.
"""

import logging
from collections import deque
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)


class OHLCVPoint:
    """Single OHLCV data point with efficient storage."""
    
    __slots__ = ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume']
    
    def __init__(self, timestamp: int, datetime_obj: datetime, 
                 open_price: float, high: float, low: float, 
                 close: float, volume: float):
        self.timestamp = timestamp
        self.datetime = datetime_obj
        self.open = open_price
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for compatibility."""
        return {
            'timestamp': self.timestamp,
            'datetime': self.datetime,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume
        }


class RollingWindow:
    """
    Efficient rolling window for OHLCV data with incremental calculations.
    
    Uses deque for O(1) append/pop operations and maintains running statistics
    for efficient indicator calculations.
    """
    
    def __init__(self, max_size: int = 500):
        self.max_size = max_size
        self.data = deque(maxlen=max_size)
        
        # Running statistics for efficient calculations
        self.running_stats = {
            'sum_close': 0.0,
            'sum_volume': 0.0,
            'sum_high': 0.0,
            'sum_low': 0.0,
            'count': 0
        }
        
        # Cached indicators (invalidated on new data)
        self._indicator_cache = {}
        self._cache_valid = False
        
    def append(self, data_point: OHLCVPoint) -> bool:
        """
        Add new data point to rolling window.
        
        Returns:
            True if data was added, False if duplicate timestamp
        """
        # Check for duplicate timestamps
        if self.data and self.data[-1].timestamp >= data_point.timestamp:
            logger.warning(f"Duplicate or out-of-order timestamp: {data_point.timestamp}")
            return False
        
        # Handle window overflow
        removed_point = None
        if len(self.data) == self.max_size:
            removed_point = self.data[0]  # Will be removed by deque
        
        # Add new point
        self.data.append(data_point)
        
        # Update running statistics
        self._update_running_stats(data_point, removed_point)
        
        # Invalidate cache
        self._cache_valid = False
        
        return True
    
    def _update_running_stats(self, added_point: OHLCVPoint, 
                            removed_point: Optional[OHLCVPoint]):
        """Update running statistics efficiently."""
        # Add new point
        self.running_stats['sum_close'] += added_point.close
        self.running_stats['sum_volume'] += added_point.volume
        self.running_stats['sum_high'] += added_point.high
        self.running_stats['sum_low'] += added_point.low
        
        # Remove old point if window is full
        if removed_point:
            self.running_stats['sum_close'] -= removed_point.close
            self.running_stats['sum_volume'] -= removed_point.volume
            self.running_stats['sum_high'] -= removed_point.high
            self.running_stats['sum_low'] -= removed_point.low
        else:
            self.running_stats['count'] += 1
    
    def get_closes(self, length: Optional[int] = None) -> np.ndarray:
        """Get close prices as numpy array."""
        if not self.data:
            return np.array([])
        
        length = length or len(self.data)
        length = min(length, len(self.data))
        
        return np.array([point.close for point in list(self.data)[-length:]])
    
    def get_highs(self, length: Optional[int] = None) -> np.ndarray:
        """Get high prices as numpy array."""
        if not self.data:
            return np.array([])
        
        length = length or len(self.data)
        length = min(length, len(self.data))
        
        return np.array([point.high for point in list(self.data)[-length:]])
    
    def get_lows(self, length: Optional[int] = None) -> np.ndarray:
        """Get low prices as numpy array."""
        if not self.data:
            return np.array([])
        
        length = length or len(self.data)
        length = min(length, len(self.data))
        
        return np.array([point.low for point in list(self.data)[-length:]])
    
    def get_volumes(self, length: Optional[int] = None) -> np.ndarray:
        """Get volumes as numpy array."""
        if not self.data:
            return np.array([])
        
        length = length or len(self.data)
        length = min(length, len(self.data))
        
        return np.array([point.volume for point in list(self.data)[-length:]])
    
    def get_simple_moving_average(self, period: int) -> Optional[float]:
        """Calculate simple moving average efficiently."""
        if len(self.data) < period:
            return None
        
        cache_key = f"sma_{period}"
        if self._cache_valid and cache_key in self._indicator_cache:
            return self._indicator_cache[cache_key]
        
        # Calculate SMA for the last 'period' points
        closes = self.get_closes(period)
        sma = np.mean(closes)
        
        # Cache result
        self._indicator_cache[cache_key] = sma
        return sma
    
    def get_exponential_moving_average(self, period: int, alpha: Optional[float] = None) -> Optional[float]:
        """Calculate exponential moving average efficiently."""
        if len(self.data) < period:
            return None
        
        if alpha is None:
            alpha = 2.0 / (period + 1)
        
        cache_key = f"ema_{period}_{alpha}"
        if self._cache_valid and cache_key in self._indicator_cache:
            return self._indicator_cache[cache_key]
        
        closes = self.get_closes()
        if len(closes) == 0:
            return None
        
        # Calculate EMA
        ema = closes[0]  # Start with first value
        for price in closes[1:]:
            ema = alpha * price + (1 - alpha) * ema
        
        # Cache result
        self._indicator_cache[cache_key] = ema
        return ema
    
    def to_dataframe(self, length: Optional[int] = None) -> pd.DataFrame:
        """Convert to pandas DataFrame for compatibility with existing strategies."""
        if not self.data:
            return pd.DataFrame()
        
        length = length or len(self.data)
        length = min(length, len(self.data))
        
        data_list = [point.to_dict() for point in list(self.data)[-length:]]
        df = pd.DataFrame(data_list)
        df.set_index('datetime', inplace=True)
        
        return df
    
    def __len__(self) -> int:
        """Return number of data points."""
        return len(self.data)
    
    def is_ready(self, min_points: int = 50) -> bool:
        """Check if window has sufficient data for analysis."""
        return len(self.data) >= min_points
    
    def get_latest(self) -> Optional[OHLCVPoint]:
        """Get the most recent data point."""
        return self.data[-1] if self.data else None
    
    def clear_cache(self):
        """Clear indicator cache."""
        self._indicator_cache.clear()
        self._cache_valid = False


class IncrementalIndicators:
    """
    Incremental indicator calculations that update efficiently with new data.
    """
    
    def __init__(self):
        self.indicators = {}
        self.last_update = None
    
    def update_sma(self, window: RollingWindow, period: int, indicator_id: str):
        """Update Simple Moving Average incrementally."""
        if not window.is_ready(period):
            return None
        
        current_sma = window.get_simple_moving_average(period)
        self.indicators[indicator_id] = current_sma
        return current_sma
    
    def update_ema(self, window: RollingWindow, period: int, indicator_id: str, alpha: Optional[float] = None):
        """Update Exponential Moving Average incrementally."""
        if not window.is_ready(period):
            return None
        
        current_ema = window.get_exponential_moving_average(period, alpha)
        self.indicators[indicator_id] = current_ema
        return current_ema
    
    def get_indicator(self, indicator_id: str) -> Optional[float]:
        """Get cached indicator value."""
        return self.indicators.get(indicator_id)
    
    def clear(self):
        """Clear all indicators."""
        self.indicators.clear()
        self.last_update = None
