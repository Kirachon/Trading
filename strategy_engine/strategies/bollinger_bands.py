"""
Bollinger Bands Strategy
"""
import pandas as pd
import numpy as np
import pandas_ta as ta
from .base_strategy import BaseStrategy
import logging

logger = logging.getLogger(__name__)

class BollingerBandsStrategy(BaseStrategy):
    """
    Bollinger Bands trading strategy.
    
    Generates buy signals when price touches lower band (oversold),
    and sell signals when price touches upper band (overbought).
    Also includes squeeze and expansion detection.
    """
    
    def __init__(self, strategy_id: str, params: dict):
        """
        Initialize Bollinger Bands strategy.
        
        Args:
            strategy_id: Unique strategy identifier
            params: Strategy parameters including:
                - period: Moving average period (default: 20)
                - std_dev: Standard deviation multiplier (default: 2)
                - squeeze_threshold: Threshold for squeeze detection (default: 0.1)
                - band_touch_threshold: Threshold for band touch (default: 0.001)
        """
        default_params = {
            'period': 20,
            'std_dev': 2.0,
            'squeeze_threshold': 0.1,
            'band_touch_threshold': 0.001,
            'mean_reversion_mode': True,  # True for mean reversion, False for breakout
            'volume_confirmation': True
        }
        default_params.update(params)
        super().__init__(strategy_id, default_params)
        
        self.period = self.params['period']
        self.std_dev = self.params['std_dev']
        self.squeeze_threshold = self.params['squeeze_threshold']
        self.band_touch_threshold = self.params['band_touch_threshold']
        self.mean_reversion_mode = self.params['mean_reversion_mode']
        self.volume_confirmation = self.params['volume_confirmation']
        
        logger.info(f"Bollinger Bands strategy initialized: period={self.period}, "
                   f"std_dev={self.std_dev}, mode={'mean_reversion' if self.mean_reversion_mode else 'breakout'}")
    
    def calculate_bollinger_bands(self, prices: pd.Series) -> dict:
        """
        Calculate Bollinger Bands components.
        
        Args:
            prices: Price series
            
        Returns:
            Dictionary with upper band, lower band, middle band, and bandwidth
        """
        try:
            # Calculate Bollinger Bands using pandas_ta
            bb = ta.bbands(prices, length=self.period, std=self.std_dev)
            
            if bb is None or bb.empty:
                logger.error("Failed to calculate Bollinger Bands")
                return {
                    'upper': pd.Series(np.nan, index=prices.index),
                    'middle': pd.Series(np.nan, index=prices.index),
                    'lower': pd.Series(np.nan, index=prices.index),
                    'bandwidth': pd.Series(np.nan, index=prices.index),
                    'percent_b': pd.Series(np.nan, index=prices.index)
                }
            
            # Extract components (column names may vary)
            upper_col = [col for col in bb.columns if 'BBU' in col][0]
            middle_col = [col for col in bb.columns if 'BBM' in col][0]
            lower_col = [col for col in bb.columns if 'BBL' in col][0]
            bandwidth_col = [col for col in bb.columns if 'BBB' in col][0]
            percent_b_col = [col for col in bb.columns if 'BBP' in col][0]
            
            return {
                'upper': bb[upper_col],
                'middle': bb[middle_col],
                'lower': bb[lower_col],
                'bandwidth': bb[bandwidth_col],
                'percent_b': bb[percent_b_col]
            }
            
        except Exception as e:
            logger.error(f"Error calculating Bollinger Bands: {e}")
            # Fallback manual calculation
            return self.manual_bollinger_calculation(prices)
    
    def manual_bollinger_calculation(self, prices: pd.Series) -> dict:
        """
        Manual Bollinger Bands calculation as fallback.
        
        Args:
            prices: Price series
            
        Returns:
            Dictionary with Bollinger Bands components
        """
        try:
            # Calculate middle band (SMA)
            middle = prices.rolling(window=self.period).mean()
            
            # Calculate standard deviation
            std = prices.rolling(window=self.period).std()
            
            # Calculate upper and lower bands
            upper = middle + (std * self.std_dev)
            lower = middle - (std * self.std_dev)
            
            # Calculate bandwidth (normalized)
            bandwidth = (upper - lower) / middle
            
            # Calculate %B (position within bands)
            percent_b = (prices - lower) / (upper - lower)
            
            return {
                'upper': upper,
                'middle': middle,
                'lower': lower,
                'bandwidth': bandwidth,
                'percent_b': percent_b
            }
            
        except Exception as e:
            logger.error(f"Error in manual Bollinger Bands calculation: {e}")
            return {
                'upper': pd.Series(np.nan, index=prices.index),
                'middle': pd.Series(np.nan, index=prices.index),
                'lower': pd.Series(np.nan, index=prices.index),
                'bandwidth': pd.Series(np.nan, index=prices.index),
                'percent_b': pd.Series(np.nan, index=prices.index)
            }
    
    def detect_squeeze(self, bb_data: dict) -> pd.Series:
        """
        Detect Bollinger Band squeeze (low volatility periods).
        
        Args:
            bb_data: Bollinger Bands data
            
        Returns:
            Series indicating squeeze periods
        """
        try:
            bandwidth = bb_data['bandwidth']
            
            # Calculate rolling minimum of bandwidth
            bandwidth_min = bandwidth.rolling(window=20).min()
            
            # Squeeze occurs when current bandwidth is near the minimum
            squeeze = bandwidth <= (bandwidth_min * (1 + self.squeeze_threshold))
            
            return squeeze
            
        except Exception as e:
            logger.error(f"Error detecting squeeze: {e}")
            return pd.Series(False, index=bb_data['bandwidth'].index)
    
    def detect_expansion(self, bb_data: dict) -> pd.Series:
        """
        Detect Bollinger Band expansion (high volatility periods).
        
        Args:
            bb_data: Bollinger Bands data
            
        Returns:
            Series indicating expansion periods
        """
        try:
            bandwidth = bb_data['bandwidth']
            
            # Calculate rolling maximum of bandwidth
            bandwidth_max = bandwidth.rolling(window=20).max()
            
            # Expansion occurs when current bandwidth is near the maximum
            expansion = bandwidth >= (bandwidth_max * 0.8)
            
            return expansion
            
        except Exception as e:
            logger.error(f"Error detecting expansion: {e}")
            return pd.Series(False, index=bb_data['bandwidth'].index)
    
    def generate_signals(self, market_data: pd.DataFrame) -> pd.Series:
        """
        Generate trading signals based on Bollinger Bands.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Series of signals (1: buy, -1: sell, 0: hold)
        """
        if not self.validate_data(market_data):
            return pd.Series(0, index=market_data.index)
        
        # Need sufficient data for Bollinger Bands calculation
        min_data_length = self.period + 20
        if len(market_data) < min_data_length:
            logger.warning(f"Insufficient data for Bollinger Bands strategy: {len(market_data)} < {min_data_length}")
            return pd.Series(0, index=market_data.index)
        
        try:
            # Calculate Bollinger Bands
            bb_data = self.calculate_bollinger_bands(market_data['close'])
            
            # Initialize signals
            signals = pd.Series(0, index=market_data.index)
            
            if self.mean_reversion_mode:
                # Mean reversion strategy
                signals = self.generate_mean_reversion_signals(market_data, bb_data)
            else:
                # Breakout strategy
                signals = self.generate_breakout_signals(market_data, bb_data)
            
            # Apply additional filters
            signals = self.apply_bb_filters(signals, bb_data, market_data)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating Bollinger Bands signals: {e}")
            return pd.Series(0, index=market_data.index)
    
    def generate_mean_reversion_signals(self, market_data: pd.DataFrame, bb_data: dict) -> pd.Series:
        """
        Generate mean reversion signals (buy at lower band, sell at upper band).
        
        Args:
            market_data: Market data
            bb_data: Bollinger Bands data
            
        Returns:
            Series of signals
        """
        try:
            signals = pd.Series(0, index=market_data.index)
            
            prices = market_data['close']
            upper_band = bb_data['upper']
            lower_band = bb_data['lower']
            percent_b = bb_data['percent_b']
            
            # Buy when price touches or goes below lower band
            lower_touch = prices <= (lower_band * (1 + self.band_touch_threshold))
            oversold_condition = percent_b <= 0.1  # %B near 0
            
            # Sell when price touches or goes above upper band
            upper_touch = prices >= (upper_band * (1 - self.band_touch_threshold))
            overbought_condition = percent_b >= 0.9  # %B near 1
            
            # Generate signals
            signals[lower_touch | oversold_condition] = 1.0
            signals[upper_touch | overbought_condition] = -1.0
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating mean reversion signals: {e}")
            return pd.Series(0, index=market_data.index)
    
    def generate_breakout_signals(self, market_data: pd.DataFrame, bb_data: dict) -> pd.Series:
        """
        Generate breakout signals (buy on upper band breakout, sell on lower band breakdown).
        
        Args:
            market_data: Market data
            bb_data: Bollinger Bands data
            
        Returns:
            Series of signals
        """
        try:
            signals = pd.Series(0, index=market_data.index)
            
            prices = market_data['close']
            upper_band = bb_data['upper']
            lower_band = bb_data['lower']
            
            # Detect squeeze periods (low volatility before breakout)
            squeeze = self.detect_squeeze(bb_data)
            
            # Buy on breakout above upper band after squeeze
            upper_breakout = (prices > upper_band) & (prices.shift(1) <= upper_band.shift(1))
            buy_condition = upper_breakout & squeeze.shift(1)
            
            # Sell on breakdown below lower band after squeeze
            lower_breakdown = (prices < lower_band) & (prices.shift(1) >= lower_band.shift(1))
            sell_condition = lower_breakdown & squeeze.shift(1)
            
            # Generate signals
            signals[buy_condition] = 1.0
            signals[sell_condition] = -1.0
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating breakout signals: {e}")
            return pd.Series(0, index=market_data.index)
    
    def apply_bb_filters(self, signals: pd.Series, bb_data: dict, 
                        market_data: pd.DataFrame) -> pd.Series:
        """
        Apply additional filters to improve signal quality.
        
        Args:
            signals: Raw signals
            bb_data: Bollinger Bands data
            market_data: Market data
            
        Returns:
            Filtered signals
        """
        try:
            # Filter 1: Volume confirmation
            if self.volume_confirmation and 'volume' in market_data.columns:
                avg_volume = market_data['volume'].rolling(window=20).mean()
                high_volume = market_data['volume'] > avg_volume * 1.2
                
                # Require high volume for signals
                signals[~high_volume] *= 0.7
            
            # Filter 2: Bandwidth filter
            bandwidth = bb_data['bandwidth']
            avg_bandwidth = bandwidth.rolling(window=20).mean()
            
            # Avoid signals during extremely low bandwidth (choppy markets)
            low_bandwidth = bandwidth < avg_bandwidth * 0.5
            signals[low_bandwidth] *= 0.5
            
            # Filter 3: %B momentum
            percent_b = bb_data['percent_b']
            pb_momentum = percent_b.diff()
            
            # For buy signals, prefer when %B is turning up
            buy_signals = signals > 0
            pb_turning_up = pb_momentum > 0
            signals[buy_signals & ~pb_turning_up] *= 0.8
            
            # For sell signals, prefer when %B is turning down
            sell_signals = signals < 0
            pb_turning_down = pb_momentum < 0
            signals[sell_signals & ~pb_turning_down] *= 0.8
            
            # Filter 4: Middle band trend
            middle_band = bb_data['middle']
            mb_trend = middle_band.diff()
            
            if self.mean_reversion_mode:
                # In mean reversion mode, prefer counter-trend signals
                buy_in_downtrend = (signals > 0) & (mb_trend < 0)
                sell_in_uptrend = (signals < 0) & (mb_trend > 0)
                
                signals[buy_in_downtrend] *= 1.1
                signals[sell_in_uptrend] *= 1.1
            else:
                # In breakout mode, prefer trend-following signals
                buy_in_uptrend = (signals > 0) & (mb_trend > 0)
                sell_in_downtrend = (signals < 0) & (mb_trend < 0)
                
                signals[buy_in_uptrend] *= 1.1
                signals[sell_in_downtrend] *= 1.1
            
            return signals
            
        except Exception as e:
            logger.error(f"Error applying Bollinger Bands filters: {e}")
            return signals
    
    def get_indicator_values(self, market_data: pd.DataFrame) -> dict:
        """
        Get current indicator values for monitoring.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Dictionary with current indicator values
        """
        if len(market_data) < self.period + 10:
            return {}
        
        try:
            bb_data = self.calculate_bollinger_bands(market_data['close'])
            
            current_price = market_data['close'].iloc[-1]
            current_upper = bb_data['upper'].iloc[-1]
            current_middle = bb_data['middle'].iloc[-1]
            current_lower = bb_data['lower'].iloc[-1]
            current_bandwidth = bb_data['bandwidth'].iloc[-1]
            current_percent_b = bb_data['percent_b'].iloc[-1]
            
            # Detect current market conditions
            squeeze = self.detect_squeeze(bb_data).iloc[-1]
            expansion = self.detect_expansion(bb_data).iloc[-1]
            
            # Calculate distances
            distance_to_upper = ((current_upper - current_price) / current_price) * 100
            distance_to_lower = ((current_price - current_lower) / current_price) * 100
            
            # Determine position
            if current_percent_b > 0.8:
                position = 'near_upper_band'
            elif current_percent_b < 0.2:
                position = 'near_lower_band'
            else:
                position = 'middle_range'
            
            return {
                'current_price': current_price,
                'upper_band': current_upper,
                'middle_band': current_middle,
                'lower_band': current_lower,
                'bandwidth': current_bandwidth,
                'percent_b': current_percent_b,
                'position': position,
                'squeeze': squeeze,
                'expansion': expansion,
                'distance_to_upper_pct': distance_to_upper,
                'distance_to_lower_pct': distance_to_lower,
                'volatility_regime': 'low' if squeeze else 'high' if expansion else 'normal'
            }
            
        except Exception as e:
            logger.error(f"Error getting indicator values: {e}")
            return {}
    
    def get_strategy_description(self) -> str:
        """Get human-readable strategy description."""
        mode = "mean reversion" if self.mean_reversion_mode else "breakout"
        return (f"Bollinger Bands Strategy (period={self.period}, std={self.std_dev}): "
                f"{mode.title()} strategy using band touches and volatility analysis.")
