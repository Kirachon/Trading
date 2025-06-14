"""
RSI (Relative Strength Index) Strategy
"""
import pandas as pd
import numpy as np
import pandas_ta as ta
from .base_strategy import BaseStrategy
import logging

logger = logging.getLogger(__name__)

class RSIStrategy(BaseStrategy):
    """
    RSI-based trading strategy.
    
    Generates buy signals when RSI is oversold (< oversold_threshold),
    and sell signals when RSI is overbought (> overbought_threshold).
    """
    
    def __init__(self, strategy_id: str, params: dict):
        """
        Initialize RSI strategy.
        
        Args:
            strategy_id: Unique strategy identifier
            params: Strategy parameters including:
                - rsi_period: RSI calculation period (default: 14)
                - oversold: Oversold threshold (default: 30)
                - overbought: Overbought threshold (default: 70)
                - rsi_smoothing: Additional smoothing period (default: None)
                - divergence_lookback: Lookback for divergence detection (default: 20)
        """
        default_params = {
            'rsi_period': 14,
            'oversold': 30,
            'overbought': 70,
            'rsi_smoothing': None,
            'divergence_lookback': 20,
            'exit_neutral': 50  # Exit when RSI returns to neutral
        }
        default_params.update(params)
        super().__init__(strategy_id, default_params)
        
        self.rsi_period = self.params['rsi_period']
        self.oversold = self.params['oversold']
        self.overbought = self.params['overbought']
        self.rsi_smoothing = self.params['rsi_smoothing']
        self.divergence_lookback = self.params['divergence_lookback']
        self.exit_neutral = self.params['exit_neutral']
        
        # Validate parameters
        if self.oversold >= self.overbought:
            raise ValueError("Oversold threshold must be less than overbought threshold")
        
        logger.info(f"RSI strategy initialized: period={self.rsi_period}, "
                   f"oversold={self.oversold}, overbought={self.overbought}")
    
    def calculate_rsi(self, prices: pd.Series) -> pd.Series:
        """
        Calculate RSI with optional smoothing.
        
        Args:
            prices: Price series
            
        Returns:
            RSI series
        """
        try:
            # Calculate basic RSI
            rsi = ta.rsi(prices, length=self.rsi_period)
            
            # Apply additional smoothing if specified
            if self.rsi_smoothing:
                rsi = rsi.rolling(window=self.rsi_smoothing).mean()
            
            return rsi
            
        except Exception as e:
            logger.error(f"Error calculating RSI: {e}")
            return pd.Series(np.nan, index=prices.index)
    
    def detect_divergence(self, prices: pd.Series, rsi: pd.Series) -> dict:
        """
        Detect bullish and bearish divergences between price and RSI.
        
        Args:
            prices: Price series
            rsi: RSI series
            
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
                recent_rsi = rsi.iloc[i-self.divergence_lookback:i+1]
                
                if recent_prices.empty or recent_rsi.empty:
                    continue
                
                # Find local minima and maxima
                price_min_idx = recent_prices.idxmin()
                price_max_idx = recent_prices.idxmax()
                rsi_min_idx = recent_rsi.idxmin()
                rsi_max_idx = recent_rsi.idxmax()
                
                # Bullish divergence: price makes lower low, RSI makes higher low
                if (price_min_idx == recent_prices.index[-1] and 
                    rsi_min_idx != recent_rsi.index[-1] and
                    recent_prices.iloc[-1] < recent_prices.iloc[0] and
                    recent_rsi.iloc[-1] > recent_rsi.iloc[0]):
                    bullish_div.iloc[i] = True
                
                # Bearish divergence: price makes higher high, RSI makes lower high
                if (price_max_idx == recent_prices.index[-1] and 
                    rsi_max_idx != recent_rsi.index[-1] and
                    recent_prices.iloc[-1] > recent_prices.iloc[0] and
                    recent_rsi.iloc[-1] < recent_rsi.iloc[0]):
                    bearish_div.iloc[i] = True
            
            return {
                'bullish_divergence': bullish_div,
                'bearish_divergence': bearish_div
            }
            
        except Exception as e:
            logger.error(f"Error detecting divergence: {e}")
            return {
                'bullish_divergence': pd.Series(False, index=prices.index),
                'bearish_divergence': pd.Series(False, index=prices.index)
            }
    
    def generate_signals(self, market_data: pd.DataFrame) -> pd.Series:
        """
        Generate trading signals based on RSI levels and divergences.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Series of signals (1: buy, -1: sell, 0: hold)
        """
        if not self.validate_data(market_data):
            return pd.Series(0, index=market_data.index)
        
        # Need sufficient data for RSI calculation
        min_data_length = self.rsi_period + (self.rsi_smoothing or 0) + 10
        if len(market_data) < min_data_length:
            logger.warning(f"Insufficient data for RSI strategy: {len(market_data)} < {min_data_length}")
            return pd.Series(0, index=market_data.index)
        
        try:
            # Calculate RSI
            rsi = self.calculate_rsi(market_data['close'])
            
            # Initialize signals
            signals = pd.Series(0, index=market_data.index)
            
            # Basic RSI signals
            oversold_condition = rsi < self.oversold
            overbought_condition = rsi > self.overbought
            
            # Generate basic signals
            signals[oversold_condition] = 1.0   # Buy when oversold
            signals[overbought_condition] = -1.0  # Sell when overbought
            
            # Detect divergences for additional signals
            divergences = self.detect_divergence(market_data['close'], rsi)
            
            # Enhance signals with divergences
            # Bullish divergence in oversold region strengthens buy signal
            strong_buy = oversold_condition & divergences['bullish_divergence']
            signals[strong_buy] = 1.0
            
            # Bearish divergence in overbought region strengthens sell signal
            strong_sell = overbought_condition & divergences['bearish_divergence']
            signals[strong_sell] = -1.0
            
            # Apply additional filters
            signals = self.apply_rsi_filters(signals, rsi, market_data)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating RSI signals: {e}")
            return pd.Series(0, index=market_data.index)
    
    def apply_rsi_filters(self, signals: pd.Series, rsi: pd.Series, 
                         market_data: pd.DataFrame) -> pd.Series:
        """
        Apply additional filters to improve RSI signal quality.
        
        Args:
            signals: Raw RSI signals
            rsi: RSI values
            market_data: Market data
            
        Returns:
            Filtered signals
        """
        try:
            # Filter 1: RSI momentum confirmation
            rsi_momentum = rsi.diff()
            
            # For buy signals, prefer when RSI is turning up from oversold
            buy_signals = signals == 1.0
            rsi_turning_up = rsi_momentum > 0
            signals[buy_signals & ~rsi_turning_up] = 0
            
            # For sell signals, prefer when RSI is turning down from overbought
            sell_signals = signals == -1.0
            rsi_turning_down = rsi_momentum < 0
            signals[sell_signals & ~rsi_turning_down] = 0
            
            # Filter 2: Multiple timeframe confirmation (if available)
            # This would require higher timeframe data
            # For now, skip this filter
            
            # Filter 3: Volume confirmation (basic)
            if 'volume' in market_data.columns:
                # Require above-average volume for signals
                avg_volume = market_data['volume'].rolling(window=20).mean()
                high_volume = market_data['volume'] > avg_volume
                
                # Reduce signal strength for low volume
                low_volume_mask = ~high_volume
                signals[low_volume_mask] *= 0.5  # Reduce signal strength
            
            # Filter 4: Trend alignment
            # Calculate simple trend using price vs moving average
            ma_period = self.params.get('trend_ma_period', 50)
            if len(market_data) >= ma_period:
                ma = market_data['close'].rolling(window=ma_period).mean()
                uptrend = market_data['close'] > ma
                downtrend = market_data['close'] < ma
                
                # Prefer buy signals in uptrends, sell signals in downtrends
                buy_in_downtrend = (signals == 1.0) & downtrend
                sell_in_uptrend = (signals == -1.0) & uptrend
                
                # Reduce signal strength for counter-trend signals
                signals[buy_in_downtrend] *= 0.7
                signals[sell_in_uptrend] *= 0.7
            
            return signals
            
        except Exception as e:
            logger.error(f"Error applying RSI filters: {e}")
            return signals
    
    def get_exit_signals(self, market_data: pd.DataFrame, entry_signals: pd.Series) -> pd.Series:
        """
        Generate exit signals based on RSI returning to neutral zone.
        
        Args:
            market_data: DataFrame with OHLCV data
            entry_signals: Entry signals to generate exits for
            
        Returns:
            Series of exit signals
        """
        try:
            rsi = self.calculate_rsi(market_data['close'])
            exit_signals = pd.Series(0, index=market_data.index)
            
            # Exit long positions when RSI crosses above neutral from below
            long_entries = entry_signals == 1.0
            rsi_above_neutral = rsi > self.exit_neutral
            rsi_was_below_neutral = rsi.shift(1) <= self.exit_neutral
            
            long_exits = long_entries.shift(1).fillna(False) & rsi_above_neutral & rsi_was_below_neutral
            exit_signals[long_exits] = -1.0
            
            # Exit short positions when RSI crosses below neutral from above
            short_entries = entry_signals == -1.0
            rsi_below_neutral = rsi < self.exit_neutral
            rsi_was_above_neutral = rsi.shift(1) >= self.exit_neutral
            
            short_exits = short_entries.shift(1).fillna(False) & rsi_below_neutral & rsi_was_above_neutral
            exit_signals[short_exits] = 1.0
            
            return exit_signals
            
        except Exception as e:
            logger.error(f"Error generating exit signals: {e}")
            return pd.Series(0, index=market_data.index)
    
    def get_indicator_values(self, market_data: pd.DataFrame) -> dict:
        """
        Get current indicator values for monitoring.
        
        Args:
            market_data: DataFrame with OHLCV data
            
        Returns:
            Dictionary with current indicator values
        """
        if len(market_data) < self.rsi_period + 10:
            return {}
        
        try:
            rsi = self.calculate_rsi(market_data['close'])
            current_rsi = rsi.iloc[-1]
            current_price = market_data['close'].iloc[-1]
            
            # RSI momentum
            rsi_momentum = rsi.diff().iloc[-1]
            
            # Determine RSI zone
            if current_rsi < self.oversold:
                rsi_zone = 'oversold'
            elif current_rsi > self.overbought:
                rsi_zone = 'overbought'
            else:
                rsi_zone = 'neutral'
            
            # Detect recent divergences
            divergences = self.detect_divergence(market_data['close'], rsi)
            recent_bullish_div = divergences['bullish_divergence'].tail(5).any()
            recent_bearish_div = divergences['bearish_divergence'].tail(5).any()
            
            return {
                'current_price': current_price,
                'rsi': current_rsi,
                'rsi_zone': rsi_zone,
                'rsi_momentum': rsi_momentum,
                'oversold_threshold': self.oversold,
                'overbought_threshold': self.overbought,
                'recent_bullish_divergence': recent_bullish_div,
                'recent_bearish_divergence': recent_bearish_div,
                'distance_to_oversold': current_rsi - self.oversold,
                'distance_to_overbought': self.overbought - current_rsi
            }
            
        except Exception as e:
            logger.error(f"Error getting indicator values: {e}")
            return {}
    
    def get_strategy_description(self) -> str:
        """Get human-readable strategy description."""
        return (f"RSI Strategy (period={self.rsi_period}): "
                f"Buy when RSI < {self.oversold} (oversold), "
                f"sell when RSI > {self.overbought} (overbought). "
                f"Enhanced with divergence detection.")
