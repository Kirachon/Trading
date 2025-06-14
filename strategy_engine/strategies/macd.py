"""
MACD (Moving Average Convergence Divergence) Strategy
"""
import pandas as pd
import numpy as np
import pandas_ta as ta
from .base_strategy import BaseStrategy
import logging

logger = logging.getLogger(__name__)

class MACDStrategy(BaseStrategy):
    """
    MACD-based trading strategy.
    
    Generates buy signals when MACD line crosses above signal line,
    and sell signals when MACD line crosses below signal line.
    """
    
    def __init__(self, strategy_id: str, params: dict):
        """
        Initialize MACD strategy.
        
        Args:
            strategy_id: Unique strategy identifier
            params: Strategy parameters including:
                - fast_period: Fast EMA period (default: 12)
                - slow_period: Slow EMA period (default: 26)
                - signal_period: Signal line EMA period (default: 9)
                - histogram_threshold: Minimum histogram value for signals (default: 0)
                - zero_line_filter: Use zero line as trend filter (default: True)
        """
        default_params = {
            'fast_period': 12,
            'slow_period': 26,
            'signal_period': 9,
            'histogram_threshold': 0,
            'zero_line_filter': True,
            'divergence_lookback': 20
        }
        default_params.update(params)
        super().__init__(strategy_id, default_params)
        
        self.fast_period = self.params['fast_period']
        self.slow_period = self.params['slow_period']
        self.signal_period = self.params['signal_period']
        self.histogram_threshold = self.params['histogram_threshold']
        self.zero_line_filter = self.params['zero_line_filter']
        self.divergence_lookback = self.params['divergence_lookback']
        
        # Validate parameters
        if self.fast_period >= self.slow_period:
            raise ValueError("Fast period must be less than slow period")
        
        logger.info(f"MACD strategy initialized: {self.fast_period}/{self.slow_period}/{self.signal_period}")
    
    def calculate_macd(self, prices: pd.Series) -> dict:
        """
        Calculate MACD components.
        
        Args:
            prices: Price series
            
        Returns:
            Dictionary with MACD, signal, and histogram series
        """
        try:
            # Calculate MACD using pandas_ta
            macd_data = ta.macd(
                prices, 
                fast=self.fast_period, 
                slow=self.slow_period, 
                signal=self.signal_period
            )
            
            if macd_data is None or macd_data.empty:
                logger.error("Failed to calculate MACD")
                return {
                    'macd': pd.Series(np.nan, index=prices.index),
                    'signal': pd.Series(np.nan, index=prices.index),
                    'histogram': pd.Series(np.nan, index=prices.index)
                }
            
            # Extract components
            macd_line = macd_data.iloc[:, 0]  # MACD line
            signal_line = macd_data.iloc[:, 1]  # Signal line
            histogram = macd_data.iloc[:, 2]  # Histogram
            
            return {
                'macd': macd_line,
                'signal': signal_line,
                'histogram': histogram
            }
            
        except Exception as e:
            logger.error(f"Error calculating MACD: {e}")
            return {
                'macd': pd.Series(np.nan, index=prices.index),
                'signal': pd.Series(np.nan, index=prices.index),
                'histogram': pd.Series(np.nan, index=prices.index)
            }
    
    def detect_macd_divergence(self, prices: pd.Series, macd: pd.Series) -> dict:
        """
        Detect bullish and bearish divergences between price and MACD.
        
        Args:
            prices: Price series
            macd: MACD line series
            
        Returns:
            Dictionary with bullish and bearish divergence signals
        """
        try:
            bullish_div = pd.Series(False, index=prices.index)
            bearish_div = pd.Series(False, index=prices.index)
            
            # Look for divergences over the specified lookback period
            for i in range(self.divergence_lookback, len(prices)):
                # Get recent data
                recent_prices = prices.iloc[i-self.divergence_lookback:i+1]
                recent_macd = macd.iloc[i-self.divergence_lookback:i+1]
                
                if recent_prices.empty or recent_macd.empty or recent_macd.isna().all():
                    continue
                
                # Find local minima and maxima
                price_min_idx = recent_prices.idxmin()
                price_max_idx = recent_prices.idxmax()
                macd_min_idx = recent_macd.idxmin()
                macd_max_idx = recent_macd.idxmax()
                
                # Bullish divergence: price makes lower low, MACD makes higher low
                if (price_min_idx == recent_prices.index[-1] and 
                    macd_min_idx != recent_macd.index[-1] and
                    recent_prices.iloc[-1] < recent_prices.iloc[0] and
                    recent_macd.iloc[-1] > recent_macd.iloc[0]):
                    bullish_div.iloc[i] = True
                
                # Bearish divergence: price makes higher high, MACD makes lower high
                if (price_max_idx == recent_prices.index[-1] and 
                    macd_max_idx != recent_macd.index[-1] and
                    recent_prices.iloc[-1] > recent_prices.iloc[0] and
                    recent_macd.iloc[-1] < recent_macd.iloc[0]):
                    bearish_div.iloc[i] = True
            
            return {
                'bullish_divergence': bullish_div,
                'bearish_divergence': bearish_div
            }
            
        except Exception as e:
            logger.error(f"Error detecting MACD divergence: {e}")
            return {
                'bullish_divergence': pd.Series(False, index=prices.index),
                'bearish_divergence': pd.Series(False, index=prices.index)
            }
    
    def generate_signals(self, market_data: pd.DataFrame) -> pd.Series:
        """
        Generate trading signals based on MACD crossovers and divergences.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Series of signals (1: buy, -1: sell, 0: hold)
        """
        if not self.validate_data(market_data):
            return pd.Series(0, index=market_data.index)
        
        # Need sufficient data for MACD calculation
        min_data_length = self.slow_period + self.signal_period + 10
        if len(market_data) < min_data_length:
            logger.warning(f"Insufficient data for MACD strategy: {len(market_data)} < {min_data_length}")
            return pd.Series(0, index=market_data.index)
        
        try:
            # Calculate MACD components
            macd_data = self.calculate_macd(market_data['close'])
            macd_line = macd_data['macd']
            signal_line = macd_data['signal']
            histogram = macd_data['histogram']
            
            # Initialize signals
            signals = pd.Series(0, index=market_data.index)
            
            # Basic MACD crossover signals
            # Buy when MACD crosses above signal line
            bullish_crossover = (macd_line > signal_line) & (macd_line.shift(1) <= signal_line.shift(1))
            
            # Sell when MACD crosses below signal line
            bearish_crossover = (macd_line < signal_line) & (macd_line.shift(1) >= signal_line.shift(1))
            
            signals[bullish_crossover] = 1.0
            signals[bearish_crossover] = -1.0
            
            # Apply zero line filter if enabled
            if self.zero_line_filter:
                # Only buy signals when MACD is above zero (uptrend)
                buy_signals = signals == 1.0
                signals[buy_signals & (macd_line <= 0)] = 0
                
                # Only sell signals when MACD is below zero (downtrend)
                sell_signals = signals == -1.0
                signals[sell_signals & (macd_line >= 0)] = 0
            
            # Apply histogram threshold filter
            if self.histogram_threshold != 0:
                buy_signals = signals == 1.0
                sell_signals = signals == -1.0
                
                signals[buy_signals & (histogram < self.histogram_threshold)] = 0
                signals[sell_signals & (histogram > -self.histogram_threshold)] = 0
            
            # Enhance with divergence signals
            divergences = self.detect_macd_divergence(market_data['close'], macd_line)
            
            # Add divergence-based signals
            signals[divergences['bullish_divergence'] & (macd_line < 0)] = 1.0
            signals[divergences['bearish_divergence'] & (macd_line > 0)] = -1.0
            
            # Apply additional filters
            signals = self.apply_macd_filters(signals, macd_data, market_data)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating MACD signals: {e}")
            return pd.Series(0, index=market_data.index)
    
    def apply_macd_filters(self, signals: pd.Series, macd_data: dict, 
                          market_data: pd.DataFrame) -> pd.Series:
        """
        Apply additional filters to improve MACD signal quality.
        
        Args:
            signals: Raw MACD signals
            macd_data: MACD components
            market_data: Market data
            
        Returns:
            Filtered signals
        """
        try:
            macd_line = macd_data['macd']
            signal_line = macd_data['signal']
            histogram = macd_data['histogram']
            
            # Filter 1: Histogram momentum confirmation
            histogram_momentum = histogram.diff()
            
            # For buy signals, prefer when histogram is increasing
            buy_signals = signals == 1.0
            histogram_increasing = histogram_momentum > 0
            signals[buy_signals & ~histogram_increasing] *= 0.8  # Reduce signal strength
            
            # For sell signals, prefer when histogram is decreasing
            sell_signals = signals == -1.0
            histogram_decreasing = histogram_momentum < 0
            signals[sell_signals & ~histogram_decreasing] *= 0.8  # Reduce signal strength
            
            # Filter 2: MACD line momentum
            macd_momentum = macd_line.diff()
            
            # Prefer signals in the direction of MACD momentum
            buy_with_momentum = buy_signals & (macd_momentum > 0)
            sell_with_momentum = sell_signals & (macd_momentum < 0)
            
            # Strengthen signals with momentum confirmation
            signals[buy_with_momentum] *= 1.2
            signals[sell_with_momentum] *= 1.2
            
            # Filter 3: Signal line slope
            signal_slope = signal_line.diff()
            
            # Avoid signals when signal line is moving against the trade direction
            buy_against_signal = buy_signals & (signal_slope < 0)
            sell_against_signal = sell_signals & (signal_slope > 0)
            
            signals[buy_against_signal] *= 0.7
            signals[sell_against_signal] *= 0.7
            
            # Filter 4: Volume confirmation (if available)
            if 'volume' in market_data.columns:
                avg_volume = market_data['volume'].rolling(window=20).mean()
                high_volume = market_data['volume'] > avg_volume * 1.2
                
                # Strengthen signals with high volume
                signals[high_volume] *= 1.1
                
                # Weaken signals with low volume
                low_volume = market_data['volume'] < avg_volume * 0.8
                signals[low_volume] *= 0.9
            
            # Ensure signals stay within valid range
            signals = signals.clip(-1.0, 1.0)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error applying MACD filters: {e}")
            return signals
    
    def get_trend_strength(self, macd_data: dict) -> pd.Series:
        """
        Calculate trend strength based on MACD components.
        
        Args:
            macd_data: MACD components
            
        Returns:
            Series indicating trend strength
        """
        try:
            macd_line = macd_data['macd']
            signal_line = macd_data['signal']
            histogram = macd_data['histogram']
            
            # Trend strength based on multiple factors
            # 1. Distance between MACD and signal line
            line_separation = abs(macd_line - signal_line)
            
            # 2. Histogram magnitude
            histogram_strength = abs(histogram)
            
            # 3. MACD line distance from zero
            zero_distance = abs(macd_line)
            
            # Combine factors (normalize to 0-1 range)
            trend_strength = (line_separation + histogram_strength + zero_distance) / 3
            
            # Normalize using rolling percentile
            trend_strength_norm = trend_strength.rolling(window=50).rank(pct=True)
            
            return trend_strength_norm.fillna(0.5)
            
        except Exception as e:
            logger.error(f"Error calculating trend strength: {e}")
            return pd.Series(0.5, index=macd_data['macd'].index)
    
    def get_indicator_values(self, market_data: pd.DataFrame) -> dict:
        """
        Get current indicator values for monitoring.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Dictionary with current indicator values
        """
        min_length = self.slow_period + self.signal_period + 10
        if len(market_data) < min_length:
            return {}
        
        try:
            macd_data = self.calculate_macd(market_data['close'])
            
            current_price = market_data['close'].iloc[-1]
            current_macd = macd_data['macd'].iloc[-1]
            current_signal = macd_data['signal'].iloc[-1]
            current_histogram = macd_data['histogram'].iloc[-1]
            
            # Calculate additional metrics
            macd_momentum = macd_data['macd'].diff().iloc[-1]
            histogram_momentum = macd_data['histogram'].diff().iloc[-1]
            
            # Trend strength
            trend_strength = self.get_trend_strength(macd_data).iloc[-1]
            
            # Determine MACD position relative to zero and signal
            macd_position = 'above_zero' if current_macd > 0 else 'below_zero'
            macd_vs_signal = 'above_signal' if current_macd > current_signal else 'below_signal'
            
            # Detect recent divergences
            divergences = self.detect_macd_divergence(market_data['close'], macd_data['macd'])
            recent_bullish_div = divergences['bullish_divergence'].tail(5).any()
            recent_bearish_div = divergences['bearish_divergence'].tail(5).any()
            
            return {
                'current_price': current_price,
                'macd': current_macd,
                'signal': current_signal,
                'histogram': current_histogram,
                'macd_momentum': macd_momentum,
                'histogram_momentum': histogram_momentum,
                'trend_strength': trend_strength,
                'macd_position': macd_position,
                'macd_vs_signal': macd_vs_signal,
                'recent_bullish_divergence': recent_bullish_div,
                'recent_bearish_divergence': recent_bearish_div,
                'crossover_imminent': abs(current_macd - current_signal) < abs(current_histogram) * 0.1
            }
            
        except Exception as e:
            logger.error(f"Error getting indicator values: {e}")
            return {}
    
    def get_strategy_description(self) -> str:
        """Get human-readable strategy description."""
        return (f"MACD Strategy ({self.fast_period}/{self.slow_period}/{self.signal_period}): "
                f"Buy when MACD crosses above signal line, sell when it crosses below. "
                f"Enhanced with divergence detection and trend filters.")
