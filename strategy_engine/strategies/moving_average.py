"""
Moving Average Crossover Strategy
"""
import pandas as pd
import numpy as np
from typing import Optional
from .base_strategy import BaseStrategy
import logging

logger = logging.getLogger(__name__)

class MovingAverageCrossover(BaseStrategy):
    """
    Moving Average Crossover strategy.
    
    Generates buy signals when short MA crosses above long MA,
    and sell signals when short MA crosses below long MA.
    """
    
    def __init__(self, strategy_id: str, params: dict):
        """
        Initialize Moving Average Crossover strategy.
        
        Args:
            strategy_id: Unique strategy identifier
            params: Strategy parameters including:
                - short_window: Short MA period (default: 50)
                - long_window: Long MA period (default: 200)
                - ma_type: Type of MA ('sma', 'ema') (default: 'sma')
        """
        default_params = {
            'short_window': 50,
            'long_window': 200,
            'ma_type': 'sma'
        }
        default_params.update(params)
        super().__init__(strategy_id, default_params)
        
        self.short_window = self.params['short_window']
        self.long_window = self.params['long_window']
        self.ma_type = self.params['ma_type']
        
        # Validate parameters
        if self.short_window >= self.long_window:
            raise ValueError("Short window must be less than long window")
        
        logger.info(f"MA Crossover strategy initialized: {self.short_window}/{self.long_window} {self.ma_type.upper()}")
    
    def calculate_moving_average(self, prices: pd.Series, window: int) -> pd.Series:
        """
        Calculate moving average based on type.
        
        Args:
            prices: Price series
            window: Moving average window
            
        Returns:
            Moving average series
        """
        if self.ma_type == 'ema':
            return prices.ewm(span=window, adjust=False).mean()
        else:  # Default to SMA
            return prices.rolling(window=window).mean()
    
    def generate_signals(self, market_data: pd.DataFrame) -> pd.Series:
        """
        Generate trading signals based on MA crossover.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Series of signals (1: buy, -1: sell, 0: hold)
        """
        if not self.validate_data(market_data):
            return pd.Series(0, index=market_data.index)
        
        # Ensure we have enough data
        if len(market_data) < self.long_window:
            logger.warning(f"Insufficient data for MA strategy: {len(market_data)} < {self.long_window}")
            return pd.Series(0, index=market_data.index)
        
        try:
            # Calculate moving averages
            short_ma = self.calculate_moving_average(market_data['close'], self.short_window)
            long_ma = self.calculate_moving_average(market_data['close'], self.long_window)
            
            # Initialize signals
            signals = pd.Series(0, index=market_data.index)
            
            # Generate crossover signals
            # Buy when short MA crosses above long MA
            bullish_crossover = (short_ma > long_ma) & (short_ma.shift(1) <= long_ma.shift(1))
            
            # Sell when short MA crosses below long MA
            bearish_crossover = (short_ma < long_ma) & (short_ma.shift(1) >= long_ma.shift(1))
            
            signals[bullish_crossover] = 1.0
            signals[bearish_crossover] = -1.0
            
            # Add trend strength filter
            signals = self.apply_trend_filter(signals, short_ma, long_ma, market_data['close'])
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating MA crossover signals: {e}")
            return pd.Series(0, index=market_data.index)
    
    def apply_trend_filter(self, signals: pd.Series, short_ma: pd.Series, 
                          long_ma: pd.Series, prices: pd.Series) -> pd.Series:
        """
        Apply additional filters to improve signal quality.
        
        Args:
            signals: Raw crossover signals
            short_ma: Short moving average
            long_ma: Long moving average
            prices: Price series
            
        Returns:
            Filtered signals
        """
        try:
            # Filter 1: Trend strength - require minimum separation between MAs
            min_separation_pct = self.params.get('min_separation_pct', 0.005)  # 0.5%
            ma_separation = abs(short_ma - long_ma) / long_ma
            
            # Only keep signals where MAs are sufficiently separated
            weak_trend_mask = ma_separation < min_separation_pct
            signals[weak_trend_mask] = 0
            
            # Filter 2: Volume confirmation (if available)
            if 'volume' in signals.index.names or hasattr(signals, 'volume'):
                # This would require volume data to be passed differently
                # For now, skip volume filter
                pass
            
            # Filter 3: Price momentum confirmation
            momentum_window = self.params.get('momentum_window', 5)
            if len(prices) >= momentum_window:
                price_momentum = prices.pct_change(momentum_window)
                
                # For buy signals, require positive momentum
                buy_signals = signals == 1.0
                signals[buy_signals & (price_momentum <= 0)] = 0
                
                # For sell signals, require negative momentum
                sell_signals = signals == -1.0
                signals[sell_signals & (price_momentum >= 0)] = 0
            
            return signals
            
        except Exception as e:
            logger.error(f"Error applying trend filter: {e}")
            return signals
    
    def get_indicator_values(self, market_data: pd.DataFrame) -> dict:
        """
        Get current indicator values for monitoring.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Dictionary with current indicator values
        """
        if len(market_data) < self.long_window:
            return {}
        
        try:
            short_ma = self.calculate_moving_average(market_data['close'], self.short_window)
            long_ma = self.calculate_moving_average(market_data['close'], self.long_window)
            
            current_price = market_data['close'].iloc[-1]
            current_short_ma = short_ma.iloc[-1]
            current_long_ma = long_ma.iloc[-1]
            
            return {
                'current_price': current_price,
                'short_ma': current_short_ma,
                'long_ma': current_long_ma,
                'ma_spread': current_short_ma - current_long_ma,
                'ma_spread_pct': ((current_short_ma - current_long_ma) / current_long_ma) * 100,
                'trend': 'bullish' if current_short_ma > current_long_ma else 'bearish'
            }
            
        except Exception as e:
            logger.error(f"Error getting indicator values: {e}")
            return {}

    def generate_signals_optimized(self, rolling_window) -> Optional[float]:
        """
        Generate signals using optimized rolling window data.

        Args:
            rolling_window: RollingWindow instance with OHLCV data

        Returns:
            Single signal value (1: buy, -1: sell, 0: hold) or None
        """
        try:
            if not rolling_window.is_ready(max(self.short_window, self.long_window) + 5):
                return None

            # Get efficient moving averages from rolling window
            short_ma = rolling_window.get_simple_moving_average(self.short_window)
            long_ma = rolling_window.get_simple_moving_average(self.long_window)

            if short_ma is None or long_ma is None:
                return None

            # Get previous values for crossover detection
            # We need to calculate previous MAs to detect crossover
            closes = rolling_window.get_closes()
            if len(closes) < max(self.short_window, self.long_window) + 1:
                return None

            # Calculate previous MAs
            prev_short_ma = np.mean(closes[-(self.short_window+1):-1])
            prev_long_ma = np.mean(closes[-(self.long_window+1):-1])

            # Detect crossovers
            current_bullish = short_ma > long_ma
            prev_bullish = prev_short_ma > prev_long_ma

            # Generate signal on crossover
            if current_bullish and not prev_bullish:
                # Bullish crossover - buy signal
                signal = 1.0
            elif not current_bullish and prev_bullish:
                # Bearish crossover - sell signal
                signal = -1.0
            else:
                # No crossover
                signal = 0.0

            # Apply trend strength filter
            if signal != 0.0:
                signal = self._apply_trend_filter_optimized(signal, short_ma, long_ma, rolling_window)

            return signal

        except Exception as e:
            logger.error(f"Error in optimized MA signal generation: {e}")
            return None

    def _apply_trend_filter_optimized(self, signal: float, short_ma: float,
                                    long_ma: float, rolling_window) -> float:
        """Apply trend strength filter using optimized data."""
        try:
            # Calculate trend strength
            ma_spread_pct = abs(short_ma - long_ma) / long_ma

            # Get recent volatility
            closes = rolling_window.get_closes(20)
            if len(closes) >= 20:
                volatility = np.std(closes) / np.mean(closes)

                # Reduce signal strength in high volatility
                if volatility > 0.05:  # 5% volatility threshold
                    signal *= 0.5

                # Require minimum trend strength
                if ma_spread_pct < 0.001:  # 0.1% minimum spread
                    signal *= 0.3

            return signal

        except Exception as e:
            logger.error(f"Error applying trend filter: {e}")
            return signal
    
    def get_strategy_description(self) -> str:
        """Get human-readable strategy description."""
        return (f"Moving Average Crossover ({self.short_window}/{self.long_window} {self.ma_type.upper()}): "
                f"Buy when {self.short_window}-period MA crosses above {self.long_window}-period MA, "
                f"sell when it crosses below.")
