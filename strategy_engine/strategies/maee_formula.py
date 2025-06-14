"""
Rayner Teo's MAEE Formula Strategy
Market Structure + Area of Value + Entry Trigger + Exits
"""
import pandas as pd
import numpy as np
import talib
from .base_strategy import BaseStrategy
import logging

logger = logging.getLogger(__name__)

class MAEEFormulaStrategy(BaseStrategy):
    """
    Implements Rayner Teo's MAEE Formula for price action trading.
    
    MAEE Components:
    - Market Structure: Use 200-period SMA to identify trend
    - Area of Value: Detect support/resistance via swing highs/lows
    - Entry Trigger: Look for candlestick patterns (hammer, engulfing)
    - Exits: Set stop-loss and take-profit using ATR
    """
    
    def __init__(self, strategy_id: str, params: dict):
        """
        Initialize MAEE Formula strategy.
        
        Args:
            strategy_id: Unique strategy identifier
            params: Strategy parameters including:
                - ma_period: Moving average period for trend (default: 200)
                - atr_period: ATR period for volatility (default: 14)
                - rr_ratio: Risk/reward ratio (default: 2.0)
                - swing_lookback: Lookback period for swing levels (default: 20)
                - support_resistance_threshold: Threshold for S/R levels (default: 0.01)
        """
        default_params = {
            'ma_period': 200,
            'atr_period': 14,
            'rr_ratio': 2.0,
            'swing_lookback': 20,
            'support_resistance_threshold': 0.01
        }
        default_params.update(params)
        super().__init__(strategy_id, default_params)
        
        self.ma_period = self.params['ma_period']
        self.atr_period = self.params['atr_period']
        self.rr_ratio = self.params['rr_ratio']
        self.swing_lookback = self.params['swing_lookback']
        self.sr_threshold = self.params['support_resistance_threshold']
        
        logger.info(f"MAEE Formula strategy initialized with MA={self.ma_period}, ATR={self.atr_period}")
    
    def identify_market_structure(self, market_data: pd.DataFrame) -> pd.Series:
        """
        Identify market structure using moving average.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Series indicating trend: 1 (uptrend), -1 (downtrend), 0 (sideways)
        """
        try:
            close_prices = market_data['close'].values
            ma = talib.SMA(close_prices, timeperiod=self.ma_period)
            ma_series = pd.Series(ma, index=market_data.index)
            
            # Determine trend
            trend = pd.Series(0, index=market_data.index)
            trend[market_data['close'] > ma_series] = 1  # Uptrend
            trend[market_data['close'] < ma_series] = -1  # Downtrend
            
            return trend
            
        except Exception as e:
            logger.error(f"Error identifying market structure: {e}")
            return pd.Series(0, index=market_data.index)
    
    def identify_swing_levels(self, market_data: pd.DataFrame) -> tuple:
        """
        Identify swing high and low levels for support/resistance.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Tuple of (support_levels, resistance_levels) as Series
        """
        try:
            highs = market_data['high']
            lows = market_data['low']
            
            # Calculate rolling max/min for swing levels
            swing_highs = highs.rolling(window=self.swing_lookback, center=True).max()
            swing_lows = lows.rolling(window=self.swing_lookback, center=True).min()
            
            # Identify actual swing points
            resistance_levels = pd.Series(np.nan, index=market_data.index)
            support_levels = pd.Series(np.nan, index=market_data.index)
            
            # Mark swing highs as resistance
            is_swing_high = (highs == swing_highs) & (highs == highs.rolling(window=self.swing_lookback).max())
            resistance_levels[is_swing_high] = highs[is_swing_high]
            
            # Mark swing lows as support
            is_swing_low = (lows == swing_lows) & (lows == lows.rolling(window=self.swing_lookback).min())
            support_levels[is_swing_low] = lows[is_swing_low]
            
            # Forward fill the levels
            resistance_levels = resistance_levels.fillna(method='ffill')
            support_levels = support_levels.fillna(method='ffill')
            
            return support_levels, resistance_levels
            
        except Exception as e:
            logger.error(f"Error identifying swing levels: {e}")
            return pd.Series(np.nan, index=market_data.index), pd.Series(np.nan, index=market_data.index)
    
    def identify_entry_triggers(self, market_data: pd.DataFrame) -> dict:
        """
        Identify candlestick patterns for entry triggers.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Dictionary with bullish and bearish pattern signals
        """
        try:
            open_prices = market_data['open'].values
            high_prices = market_data['high'].values
            low_prices = market_data['low'].values
            close_prices = market_data['close'].values
            
            # Bullish patterns
            hammer = talib.CDLHAMMER(open_prices, high_prices, low_prices, close_prices)
            bullish_engulfing = talib.CDLENGULFING(open_prices, high_prices, low_prices, close_prices)
            morning_star = talib.CDLMORNINGSTAR(open_prices, high_prices, low_prices, close_prices)
            
            # Bearish patterns
            shooting_star = talib.CDLSHOOTINGSTAR(open_prices, high_prices, low_prices, close_prices)
            bearish_engulfing = talib.CDLENGULFING(open_prices, high_prices, low_prices, close_prices)
            evening_star = talib.CDLEVENINGSTAR(open_prices, high_prices, low_prices, close_prices)
            
            # Combine patterns
            bullish_patterns = pd.Series(
                (hammer > 0) | (bullish_engulfing > 0) | (morning_star > 0),
                index=market_data.index
            )
            
            bearish_patterns = pd.Series(
                (shooting_star > 0) | (bearish_engulfing < 0) | (evening_star > 0),
                index=market_data.index
            )
            
            return {
                'bullish_patterns': bullish_patterns,
                'bearish_patterns': bearish_patterns
            }
            
        except Exception as e:
            logger.error(f"Error identifying entry triggers: {e}")
            return {
                'bullish_patterns': pd.Series(False, index=market_data.index),
                'bearish_patterns': pd.Series(False, index=market_data.index)
            }
    
    def calculate_atr(self, market_data: pd.DataFrame) -> pd.Series:
        """
        Calculate Average True Range for volatility measurement.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            ATR series
        """
        try:
            high_prices = market_data['high'].values
            low_prices = market_data['low'].values
            close_prices = market_data['close'].values
            
            atr = talib.ATR(high_prices, low_prices, close_prices, timeperiod=self.atr_period)
            return pd.Series(atr, index=market_data.index)
            
        except Exception as e:
            logger.error(f"Error calculating ATR: {e}")
            return pd.Series(0, index=market_data.index)
    
    def generate_signals(self, market_data: pd.DataFrame) -> pd.Series:
        """
        Generate trading signals using MAEE Formula.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Series of signals (1: buy, -1: sell, 0: hold)
        """
        if not self.validate_data(market_data):
            return pd.Series(0, index=market_data.index)
        
        # Need sufficient data for all indicators
        min_data_length = max(self.ma_period, self.swing_lookback, self.atr_period) + 10
        if len(market_data) < min_data_length:
            logger.warning(f"Insufficient data for MAEE strategy: {len(market_data)} < {min_data_length}")
            return pd.Series(0, index=market_data.index)
        
        try:
            # Initialize signals
            signals = pd.Series(0, index=market_data.index)
            
            # Step 1: Market Structure
            trend = self.identify_market_structure(market_data)
            
            # Step 2: Area of Value (Support/Resistance)
            support_levels, resistance_levels = self.identify_swing_levels(market_data)
            
            # Step 3: Entry Triggers (Candlestick Patterns)
            patterns = self.identify_entry_triggers(market_data)
            
            # Step 4: Combine conditions for signals
            current_price = market_data['close']
            
            # Long conditions: Uptrend + Near Support + Bullish Pattern
            near_support = (current_price <= support_levels * (1 + self.sr_threshold)) & \
                          (current_price >= support_levels * (1 - self.sr_threshold))
            
            long_conditions = (trend >= 0) & near_support & patterns['bullish_patterns']
            
            # Short conditions: Downtrend + Near Resistance + Bearish Pattern
            near_resistance = (current_price >= resistance_levels * (1 - self.sr_threshold)) & \
                             (current_price <= resistance_levels * (1 + self.sr_threshold))
            
            short_conditions = (trend <= 0) & near_resistance & patterns['bearish_patterns']
            
            # Apply conditions
            signals[long_conditions] = 1.0
            signals[short_conditions] = -1.0
            
            # Apply additional filters
            signals = self.apply_additional_filters(signals, market_data, trend)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating MAEE signals: {e}")
            return pd.Series(0, index=market_data.index)
    
    def apply_additional_filters(self, signals: pd.Series, market_data: pd.DataFrame, 
                               trend: pd.Series) -> pd.Series:
        """
        Apply additional filters to improve signal quality.
        
        Args:
            signals: Raw signals
            market_data: Market data
            trend: Trend series
            
        Returns:
            Filtered signals
        """
        try:
            # Filter 1: Trend strength
            # Only take long signals in strong uptrends, short signals in strong downtrends
            ma = talib.SMA(market_data['close'].values, timeperiod=self.ma_period)
            ma_series = pd.Series(ma, index=market_data.index)
            
            # Calculate trend strength
            price_ma_diff = (market_data['close'] - ma_series) / ma_series
            strong_uptrend = price_ma_diff > 0.02  # 2% above MA
            strong_downtrend = price_ma_diff < -0.02  # 2% below MA
            
            # Filter signals based on trend strength
            long_signals = signals == 1.0
            short_signals = signals == -1.0
            
            signals[long_signals & ~strong_uptrend] = 0
            signals[short_signals & ~strong_downtrend] = 0
            
            # Filter 2: ATR-based volatility filter
            atr = self.calculate_atr(market_data)
            atr_pct = atr / market_data['close']
            
            # Avoid signals during extremely low or high volatility
            min_volatility = self.params.get('min_volatility', 0.005)  # 0.5%
            max_volatility = self.params.get('max_volatility', 0.05)   # 5%
            
            low_vol_mask = atr_pct < min_volatility
            high_vol_mask = atr_pct > max_volatility
            
            signals[low_vol_mask | high_vol_mask] = 0
            
            return signals
            
        except Exception as e:
            logger.error(f"Error applying additional filters: {e}")
            return signals
    
    def calculate_exits(self, market_data: pd.DataFrame, signal: pd.Series) -> tuple:
        """
        Calculate stop loss and take profit levels using ATR.
        
        Args:
            market_data: DataFrame with OHLCV data
            signal: Trading signals
            
        Returns:
            Tuple of (stop_loss, take_profit) series
        """
        try:
            atr = self.calculate_atr(market_data)
            
            stop_loss = pd.Series(np.nan, index=market_data.index)
            take_profit = pd.Series(np.nan, index=market_data.index)
            
            for i in range(len(market_data)):
                if signal.iloc[i] == 1.0:  # Long signal
                    entry_price = market_data['close'].iloc[i]
                    sl = entry_price - (atr.iloc[i] * 2)  # 2 ATR stop loss
                    tp = entry_price + (atr.iloc[i] * 2 * self.rr_ratio)  # Risk/reward ratio
                    
                    stop_loss.iloc[i] = sl
                    take_profit.iloc[i] = tp
                    
                elif signal.iloc[i] == -1.0:  # Short signal
                    entry_price = market_data['close'].iloc[i]
                    sl = entry_price + (atr.iloc[i] * 2)  # 2 ATR stop loss
                    tp = entry_price - (atr.iloc[i] * 2 * self.rr_ratio)  # Risk/reward ratio
                    
                    stop_loss.iloc[i] = sl
                    take_profit.iloc[i] = tp
            
            return stop_loss, take_profit
            
        except Exception as e:
            logger.error(f"Error calculating exits: {e}")
            return pd.Series(np.nan, index=market_data.index), pd.Series(np.nan, index=market_data.index)
    
    def get_indicator_values(self, market_data: pd.DataFrame) -> dict:
        """
        Get current indicator values for monitoring.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Dictionary with current indicator values
        """
        if len(market_data) < max(self.ma_period, self.atr_period):
            return {}
        
        try:
            # Current values
            current_price = market_data['close'].iloc[-1]
            
            # Market structure
            trend = self.identify_market_structure(market_data)
            current_trend = trend.iloc[-1]
            
            # Support/Resistance
            support_levels, resistance_levels = self.identify_swing_levels(market_data)
            current_support = support_levels.iloc[-1] if not pd.isna(support_levels.iloc[-1]) else None
            current_resistance = resistance_levels.iloc[-1] if not pd.isna(resistance_levels.iloc[-1]) else None
            
            # ATR
            atr = self.calculate_atr(market_data)
            current_atr = atr.iloc[-1]
            
            # Moving Average
            ma = talib.SMA(market_data['close'].values, timeperiod=self.ma_period)
            current_ma = ma[-1]
            
            return {
                'current_price': current_price,
                'trend': 'bullish' if current_trend > 0 else 'bearish' if current_trend < 0 else 'sideways',
                'moving_average': current_ma,
                'support_level': current_support,
                'resistance_level': current_resistance,
                'atr': current_atr,
                'atr_pct': (current_atr / current_price) * 100,
                'distance_to_support': ((current_price - current_support) / current_support * 100) if current_support else None,
                'distance_to_resistance': ((current_resistance - current_price) / current_price * 100) if current_resistance else None
            }
            
        except Exception as e:
            logger.error(f"Error getting indicator values: {e}")
            return {}
    
    def get_strategy_description(self) -> str:
        """Get human-readable strategy description."""
        return (f"MAEE Formula Strategy: Market Structure (MA{self.ma_period}) + "
                f"Area of Value (S/R levels) + Entry Trigger (candlestick patterns) + "
                f"Exits (ATR-based with {self.rr_ratio}:1 R/R)")
