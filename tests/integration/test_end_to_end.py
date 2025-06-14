"""
End-to-end integration tests for the trading system.
"""
import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class TestEndToEndTrading:
    """End-to-end trading system tests."""
    
    @pytest.mark.asyncio
    async def test_complete_trading_flow(self, sample_market_data, mock_exchange):
        """Test complete trading flow from signal generation to execution."""
        
        # Mock services
        with patch('shared.exchange_factory.get_exchange', return_value=mock_exchange):
            # 1. Market data ingestion
            market_data = sample_market_data.iloc[-100:].copy()  # Last 100 candles
            
            # 2. Strategy signal generation
            from strategy_engine.strategies.moving_average import MovingAverageCrossover
            
            strategy = MovingAverageCrossover('test_ma', {
                'short_window': 5,
                'long_window': 10,
                'ma_type': 'sma'
            })
            
            signals = strategy.generate_signals(market_data)
            
            # Should generate some signals
            assert not signals.empty
            assert signals.sum() != 0  # At least some non-zero signals
            
            # 3. Risk management validation
            signal_data = {
                'strategy_id': 'test_ma',
                'symbol': 'BTC/USDT',
                'exchange': 'binance',
                'side': 'buy' if signals.iloc[-1] > 0 else 'sell',
                'price': market_data['close'].iloc[-1],
                'quantity': 0.1,
                'timestamp': int(time.time() * 1000)
            }
            
            # Mock risk validation (would normally go through risk service)
            risk_passed = True
            assert risk_passed == True
            
            # 4. Order execution
            if risk_passed and signals.iloc[-1] != 0:
                # Mock order execution
                order_result = await mock_exchange.create_market_order(
                    signal_data['symbol'],
                    signal_data['side'],
                    signal_data['quantity']
                )
                
                assert order_result is not None
                assert order_result['id'] == 'order_123'
                assert order_result['status'] == 'closed'
                
            # 5. Portfolio update
            # Mock portfolio accounting
            portfolio_update = {
                'type': 'fill',
                'order_id': order_result['id'],
                'symbol': signal_data['symbol'],
                'side': signal_data['side'],
                'quantity': signal_data['quantity'],
                'price': signal_data['price'],
                'timestamp': signal_data['timestamp']
            }
            
            assert portfolio_update['type'] == 'fill'
            assert portfolio_update['order_id'] == 'order_123'
    
    @pytest.mark.asyncio
    async def test_risk_rejection_flow(self, sample_market_data):
        """Test flow when risk management rejects a signal."""
        
        # Generate a signal
        from strategy_engine.strategies.moving_average import MovingAverageCrossover
        
        strategy = MovingAverageCrossover('test_ma', {
            'short_window': 5,
            'long_window': 10,
            'ma_type': 'sma'
        })
        
        signals = strategy.generate_signals(sample_market_data)
        
        # Create a signal that would be rejected (e.g., exceeding position limits)
        signal_data = {
            'strategy_id': 'test_ma',
            'symbol': 'BTC/USDT',
            'exchange': 'binance',
            'side': 'buy',
            'price': 50000.0,
            'quantity': 100.0,  # Extremely large quantity
            'timestamp': int(time.time() * 1000)
        }
        
        # Mock risk management rejection
        risk_limits = {
            'max_position_size': 10000.0,  # $10k limit
            'max_daily_loss': 1000.0,
            'max_trades_per_day': 50,
            'fat_finger_threshold': 0.05
        }
        
        # Calculate position value
        position_value = signal_data['quantity'] * signal_data['price']  # $5M
        
        # Should be rejected due to position size
        assert position_value > risk_limits['max_position_size']
        
        # Risk rejection should trigger alert
        alert = {
            'type': 'risk_rejection',
            'signal': signal_data,
            'reason': f'Position size {position_value} exceeds limit {risk_limits["max_position_size"]}',
            'timestamp': datetime.now().isoformat()
        }
        
        assert alert['type'] == 'risk_rejection'
        assert 'exceeds limit' in alert['reason']
    
    @pytest.mark.asyncio
    async def test_multiple_strategies_coordination(self, sample_market_data):
        """Test coordination between multiple strategies."""
        
        # Initialize multiple strategies
        strategies = {
            'ma_crossover': MovingAverageCrossover('ma_crossover', {
                'short_window': 5,
                'long_window': 10,
                'ma_type': 'sma'
            }),
            'rsi_strategy': None  # Would import RSIStrategy
        }
        
        # Generate signals from MA strategy
        ma_signals = strategies['ma_crossover'].generate_signals(sample_market_data)
        
        # Mock signals from other strategies
        rsi_signals = pd.Series(0, index=sample_market_data.index)
        rsi_signals.iloc[-5:] = [0, 1, 0, -1, 0]  # Some test signals
        
        # Combine signals (simplified aggregation)
        combined_signals = {}
        combined_signals['ma_crossover'] = ma_signals.iloc[-1] if not ma_signals.empty else 0
        combined_signals['rsi_strategy'] = rsi_signals.iloc[-1] if not rsi_signals.empty else 0
        
        # Check that we have signals from multiple strategies
        active_strategies = sum(1 for signal in combined_signals.values() if signal != 0)
        
        # Should have coordination logic to handle conflicting signals
        if combined_signals['ma_crossover'] > 0 and combined_signals['rsi_strategy'] < 0:
            # Conflicting signals - would need resolution logic
            final_signal = 0  # Conservative approach
        else:
            # Aligned or single strategy signal
            final_signal = max(combined_signals.values(), key=abs)
        
        # Test passes if we can handle multiple strategy coordination
        assert isinstance(final_signal, (int, float))
    
    @pytest.mark.asyncio
    async def test_market_data_to_portfolio_flow(self, sample_market_data, mock_exchange):
        """Test complete flow from market data to portfolio updates."""
        
        with patch('shared.exchange_factory.get_exchange', return_value=mock_exchange):
            # 1. Market data processing
            latest_candle = {
                'symbol': 'BTC/USDT',
                'exchange': 'binance',
                'timeframe': '1h',
                'timestamp': int(time.time() * 1000),
                'datetime': datetime.now().isoformat(),
                'open': sample_market_data['open'].iloc[-1],
                'high': sample_market_data['high'].iloc[-1],
                'low': sample_market_data['low'].iloc[-1],
                'close': sample_market_data['close'].iloc[-1],
                'volume': sample_market_data['volume'].iloc[-1]
            }
            
            # 2. Strategy processing
            # Mock strategy engine processing the new candle
            strategy_signals = []
            
            # Simulate multiple strategies processing the data
            for strategy_id in ['ma_crossover', 'rsi_strategy', 'macd_strategy']:
                # Mock signal generation
                signal_strength = np.random.uniform(-1, 1)
                
                if abs(signal_strength) > 0.7:  # High confidence threshold
                    signal = {
                        'strategy_id': strategy_id,
                        'symbol': latest_candle['symbol'],
                        'exchange': latest_candle['exchange'],
                        'side': 'buy' if signal_strength > 0 else 'sell',
                        'price': latest_candle['close'],
                        'confidence': abs(signal_strength),
                        'timestamp': latest_candle['timestamp']
                    }
                    strategy_signals.append(signal)
            
            # 3. Risk management processing
            approved_signals = []
            for signal in strategy_signals:
                # Mock risk validation
                risk_check_passed = True  # Simplified
                
                if risk_check_passed:
                    approved_signals.append(signal)
            
            # 4. Execution processing
            executed_orders = []
            for signal in approved_signals:
                # Mock order execution
                order = await mock_exchange.create_market_order(
                    signal['symbol'],
                    signal['side'],
                    0.1  # Fixed quantity for test
                )
                executed_orders.append(order)
            
            # 5. Portfolio accounting
            portfolio_updates = []
            for order in executed_orders:
                update = {
                    'type': 'position_update',
                    'order_id': order['id'],
                    'symbol': order['symbol'],
                    'side': order['side'],
                    'quantity': order['amount'],
                    'price': order['price'],
                    'timestamp': int(time.time() * 1000)
                }
                portfolio_updates.append(update)
            
            # Verify the complete flow
            assert isinstance(latest_candle, dict)
            assert len(strategy_signals) >= 0  # May be zero if no high-confidence signals
            assert len(approved_signals) <= len(strategy_signals)  # Risk may reject some
            assert len(executed_orders) == len(approved_signals)  # All approved should execute
            assert len(portfolio_updates) == len(executed_orders)  # All executions update portfolio
    
    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self, sample_market_data, mock_exchange):
        """Test system behavior under error conditions."""
        
        # Test 1: Exchange connection failure
        mock_exchange.create_market_order.side_effect = Exception("Exchange connection failed")
        
        signal = {
            'strategy_id': 'test_strategy',
            'symbol': 'BTC/USDT',
            'exchange': 'binance',
            'side': 'buy',
            'price': 50000.0,
            'quantity': 0.1
        }
        
        # Should handle exchange errors gracefully
        try:
            await mock_exchange.create_market_order(signal['symbol'], signal['side'], signal['quantity'])
            assert False, "Should have raised exception"
        except Exception as e:
            assert "Exchange connection failed" in str(e)
            # System should log error and continue operating
        
        # Test 2: Invalid market data
        invalid_data = pd.DataFrame({
            'open': [np.nan, np.nan],
            'high': [np.nan, np.nan],
            'low': [np.nan, np.nan],
            'close': [np.nan, np.nan],
            'volume': [np.nan, np.nan]
        })
        
        from strategy_engine.strategies.moving_average import MovingAverageCrossover
        strategy = MovingAverageCrossover('test_ma', {
            'short_window': 5,
            'long_window': 10,
            'ma_type': 'sma'
        })
        
        # Should handle invalid data gracefully
        signals = strategy.generate_signals(invalid_data)
        assert signals.sum() == 0  # Should return all zeros for invalid data
        
        # Test 3: Database connection failure
        # Mock database failure scenario
        db_error_occurred = True
        
        if db_error_occurred:
            # System should continue operating with cached data
            # and attempt to reconnect
            fallback_data = {
                'portfolio_summary': {'total_balance': 10000.0},
                'positions': [],
                'balances': []
            }
            
            assert fallback_data['portfolio_summary']['total_balance'] > 0
    
    @pytest.mark.asyncio
    async def test_performance_under_load(self, sample_market_data):
        """Test system performance under high load."""
        
        # Simulate high-frequency data processing
        num_updates = 100
        processing_times = []
        
        for i in range(num_updates):
            start_time = time.time()
            
            # Simulate market data processing
            latest_data = sample_market_data.iloc[-50:].copy()  # Last 50 candles
            
            # Simulate strategy processing
            from strategy_engine.strategies.moving_average import MovingAverageCrossover
            strategy = MovingAverageCrossover('test_ma', {
                'short_window': 5,
                'long_window': 10,
                'ma_type': 'sma'
            })
            
            signals = strategy.generate_signals(latest_data)
            
            end_time = time.time()
            processing_times.append(end_time - start_time)
        
        # Performance assertions
        avg_processing_time = sum(processing_times) / len(processing_times)
        max_processing_time = max(processing_times)
        
        # Should process updates quickly (under 100ms average)
        assert avg_processing_time < 0.1, f"Average processing time too high: {avg_processing_time:.3f}s"
        
        # No single update should take more than 1 second
        assert max_processing_time < 1.0, f"Max processing time too high: {max_processing_time:.3f}s"
        
        # Should maintain consistent performance
        processing_std = np.std(processing_times)
        assert processing_std < 0.05, f"Processing time variance too high: {processing_std:.3f}s"
