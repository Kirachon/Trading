"""
Strategy Controller for Streamlit Dashboard
Handles strategy management and control operations.
"""
import os
import sys
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

# Add shared modules to path
sys.path.append('/app')
from data_access import DataAccess

logger = logging.getLogger(__name__)

class StrategyController:
    """Strategy management interface for dashboard."""
    
    def __init__(self):
        self.data_access = DataAccess()
        
        # Cache for strategy data
        self.cache = {
            'strategy_status': {},
            'strategy_performance': {},
            'last_refresh': None
        }
        
        self.cache_ttl = 30  # Cache TTL in seconds
        
        # Available strategies
        self.available_strategies = [
            'ma_crossover',
            'maee_formula', 
            'rsi_strategy',
            'macd_strategy',
            'bollinger_bands'
        ]
    
    def is_cache_valid(self) -> bool:
        """Check if cache is still valid."""
        if not self.cache['last_refresh']:
            return False
        
        elapsed = (datetime.now() - self.cache['last_refresh']).total_seconds()
        return elapsed < self.cache_ttl
    
    def refresh_status(self):
        """Force refresh of strategy status."""
        self.cache['last_refresh'] = None
        self.get_strategy_status()
    
    def get_strategy_status(self) -> Dict[str, Any]:
        """Get current strategy status."""
        if self.is_cache_valid() and self.cache['strategy_status']:
            return self.cache['strategy_status']
        
        try:
            # In production, this would query the actual strategy engine
            # For now, simulate strategy status
            status = {
                'strategies_enabled': True,
                'active_strategies': self.available_strategies,
                'strategy_info': {}
            }
            
            # Simulate strategy information
            for strategy_id in self.available_strategies:
                status['strategy_info'][strategy_id] = {
                    'strategy_id': strategy_id,
                    'name': self.get_strategy_display_name(strategy_id),
                    'last_signal': 0.0,
                    'last_signal_time': None,
                    'current_position': 'flat',
                    'parameters': self.get_default_parameters(strategy_id),
                    'enabled': True,
                    'performance': self.get_strategy_performance_summary(strategy_id)
                }
            
            self.cache['strategy_status'] = status
            self.cache['last_refresh'] = datetime.now()
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting strategy status: {e}")
            return {
                'strategies_enabled': False,
                'active_strategies': [],
                'strategy_info': {}
            }
    
    def get_strategy_display_name(self, strategy_id: str) -> str:
        """Get display name for strategy."""
        name_mapping = {
            'ma_crossover': 'Moving Average Crossover',
            'maee_formula': 'MAEE Formula (Rayner Teo)',
            'rsi_strategy': 'RSI Strategy',
            'macd_strategy': 'MACD Strategy',
            'bollinger_bands': 'Bollinger Bands'
        }
        return name_mapping.get(strategy_id, strategy_id.replace('_', ' ').title())
    
    def get_default_parameters(self, strategy_id: str) -> Dict[str, Any]:
        """Get default parameters for strategy."""
        parameters = {
            'ma_crossover': {
                'short_window': 50,
                'long_window': 200,
                'ma_type': 'sma'
            },
            'maee_formula': {
                'ma_period': 200,
                'atr_period': 14,
                'rr_ratio': 2.0,
                'swing_lookback': 20
            },
            'rsi_strategy': {
                'rsi_period': 14,
                'oversold': 30,
                'overbought': 70
            },
            'macd_strategy': {
                'fast_period': 12,
                'slow_period': 26,
                'signal_period': 9
            },
            'bollinger_bands': {
                'period': 20,
                'std_dev': 2.0,
                'mean_reversion_mode': True
            }
        }
        return parameters.get(strategy_id, {})
    
    def get_strategy_performance_summary(self, strategy_id: str, days: int = 7) -> Dict[str, Any]:
        """Get performance summary for a strategy."""
        try:
            performance = self.data_access.get_strategy_performance(strategy_id, days)
            
            if 'error' in performance or 'no_data' in performance:
                return {
                    'total_trades': 0,
                    'win_rate': 0.0,
                    'total_pnl': 0.0,
                    'status': 'no_data'
                }
            
            return {
                'total_trades': performance.get('total_trades', 0),
                'win_rate': performance.get('win_rate', 0.0) * 100,  # Convert to percentage
                'total_pnl': performance.get('total_pnl', 0.0),
                'avg_win': performance.get('avg_win', 0.0),
                'avg_loss': performance.get('avg_loss', 0.0),
                'max_win': performance.get('max_win', 0.0),
                'max_loss': performance.get('max_loss', 0.0),
                'status': 'active' if performance.get('total_trades', 0) > 0 else 'inactive'
            }
            
        except Exception as e:
            logger.error(f"Error getting strategy performance for {strategy_id}: {e}")
            return {
                'total_trades': 0,
                'win_rate': 0.0,
                'total_pnl': 0.0,
                'status': 'error'
            }
    
    def get_strategy_list(self) -> List[str]:
        """Get list of available strategies."""
        return self.available_strategies.copy()
    
    def get_strategy_details(self, strategy_id: str) -> Dict[str, Any]:
        """Get detailed information for a specific strategy."""
        status = self.get_strategy_status()
        strategy_info = status.get('strategy_info', {}).get(strategy_id, {})
        
        if not strategy_info:
            return {'error': f'Strategy {strategy_id} not found'}
        
        # Get extended performance data
        performance_7d = self.data_access.get_strategy_performance(strategy_id, 7)
        performance_30d = self.data_access.get_strategy_performance(strategy_id, 30)
        
        # Get recent trades for this strategy
        recent_trades = self.data_access.get_trade_history({
            'strategy_id': strategy_id,
            'start_date': datetime.now() - timedelta(days=7)
        })
        
        return {
            **strategy_info,
            'performance_7d': performance_7d,
            'performance_30d': performance_30d,
            'recent_trades': recent_trades[:10],  # Last 10 trades
            'description': self.get_strategy_description(strategy_id)
        }
    
    def get_strategy_description(self, strategy_id: str) -> str:
        """Get description for strategy."""
        descriptions = {
            'ma_crossover': 'Generates buy signals when short MA crosses above long MA, and sell signals when short MA crosses below long MA.',
            'maee_formula': 'Implements Rayner Teo\'s MAEE Formula: Market Structure + Area of Value + Entry Trigger + Exits.',
            'rsi_strategy': 'Uses RSI indicator to identify overbought/oversold conditions with divergence detection.',
            'macd_strategy': 'MACD crossover strategy with histogram and zero-line filters for trend confirmation.',
            'bollinger_bands': 'Mean reversion strategy using Bollinger Bands with squeeze and expansion detection.'
        }
        return descriptions.get(strategy_id, 'No description available.')
    
    def enable_strategy(self, strategy_id: str):
        """Enable a specific strategy."""
        try:
            command = {
                'type': 'enable_strategy',
                'strategy_id': strategy_id
            }
            self.data_access.send_command('strategy', command)
            logger.info(f"Enabled strategy: {strategy_id}")
            
            # Refresh cache
            self.refresh_status()
            
        except Exception as e:
            logger.error(f"Error enabling strategy {strategy_id}: {e}")
    
    def disable_strategy(self, strategy_id: str):
        """Disable a specific strategy."""
        try:
            command = {
                'type': 'disable_strategy',
                'strategy_id': strategy_id
            }
            self.data_access.send_command('strategy', command)
            logger.info(f"Disabled strategy: {strategy_id}")
            
            # Refresh cache
            self.refresh_status()
            
        except Exception as e:
            logger.error(f"Error disabling strategy {strategy_id}: {e}")
    
    def enable_all_strategies(self):
        """Enable all strategies."""
        try:
            command = {
                'type': 'enable_strategies'
            }
            self.data_access.send_command('strategy', command)
            logger.info("Enabled all strategies")
            
            # Refresh cache
            self.refresh_status()
            
        except Exception as e:
            logger.error(f"Error enabling all strategies: {e}")
    
    def disable_all_strategies(self):
        """Disable all strategies."""
        try:
            command = {
                'type': 'disable_strategies'
            }
            self.data_access.send_command('strategy', command)
            logger.info("Disabled all strategies")
            
            # Refresh cache
            self.refresh_status()
            
        except Exception as e:
            logger.error(f"Error disabling all strategies: {e}")
    
    def reset_strategy(self, strategy_id: str):
        """Reset a specific strategy."""
        try:
            command = {
                'type': 'reset_strategy',
                'strategy_id': strategy_id
            }
            self.data_access.send_command('strategy', command)
            logger.info(f"Reset strategy: {strategy_id}")
            
            # Refresh cache
            self.refresh_status()
            
        except Exception as e:
            logger.error(f"Error resetting strategy {strategy_id}: {e}")
    
    def update_strategy_config(self, strategy_id: str, new_config: Dict[str, Any]):
        """Update strategy configuration."""
        try:
            command = {
                'type': 'update_strategy_config',
                'strategy_id': strategy_id,
                'config': new_config
            }
            self.data_access.send_command('strategy', command)
            logger.info(f"Updated config for strategy: {strategy_id}")
            
            # Refresh cache
            self.refresh_status()
            
        except Exception as e:
            logger.error(f"Error updating strategy config for {strategy_id}: {e}")
    
    def emergency_stop(self):
        """Emergency stop all trading activities."""
        try:
            # Disable all strategies
            self.disable_all_strategies()
            
            # Send emergency stop to execution service
            command = {
                'type': 'disable_execution'
            }
            self.data_access.send_command('execution', command)
            
            # Send emergency stop to risk management
            command = {
                'type': 'disable_risk_checks'
            }
            self.data_access.send_command('risk', command)
            
            logger.warning("EMERGENCY STOP ACTIVATED")
            
        except Exception as e:
            logger.error(f"Error during emergency stop: {e}")
    
    def get_strategy_comparison(self, days: int = 30) -> Dict[str, Any]:
        """Get performance comparison across all strategies."""
        try:
            comparison = {}
            
            for strategy_id in self.available_strategies:
                performance = self.data_access.get_strategy_performance(strategy_id, days)
                
                if 'error' not in performance and 'no_data' not in performance:
                    comparison[strategy_id] = {
                        'name': self.get_strategy_display_name(strategy_id),
                        'total_trades': performance.get('total_trades', 0),
                        'win_rate': performance.get('win_rate', 0.0),
                        'total_pnl': performance.get('total_pnl', 0.0),
                        'avg_win': performance.get('avg_win', 0.0),
                        'avg_loss': performance.get('avg_loss', 0.0),
                        'max_win': performance.get('max_win', 0.0),
                        'max_loss': performance.get('max_loss', 0.0)
                    }
                else:
                    comparison[strategy_id] = {
                        'name': self.get_strategy_display_name(strategy_id),
                        'total_trades': 0,
                        'win_rate': 0.0,
                        'total_pnl': 0.0,
                        'status': 'no_data'
                    }
            
            return comparison
            
        except Exception as e:
            logger.error(f"Error getting strategy comparison: {e}")
            return {}
    
    def get_strategy_signals_history(self, strategy_id: str, days: int = 7) -> List[Dict[str, Any]]:
        """Get recent signals for a strategy."""
        try:
            # In production, this would query the signal logs from MongoDB
            # For now, return empty list as signals are not stored in PostgreSQL
            return []
            
        except Exception as e:
            logger.error(f"Error getting signals history for {strategy_id}: {e}")
            return []
    
    def validate_strategy_parameters(self, strategy_id: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Validate strategy parameters."""
        try:
            validation_rules = {
                'ma_crossover': {
                    'short_window': {'type': int, 'min': 1, 'max': 100},
                    'long_window': {'type': int, 'min': 50, 'max': 500},
                    'ma_type': {'type': str, 'values': ['sma', 'ema']}
                },
                'rsi_strategy': {
                    'rsi_period': {'type': int, 'min': 2, 'max': 50},
                    'oversold': {'type': int, 'min': 10, 'max': 40},
                    'overbought': {'type': int, 'min': 60, 'max': 90}
                }
                # Add more validation rules as needed
            }
            
            rules = validation_rules.get(strategy_id, {})
            errors = []
            
            for param, value in parameters.items():
                if param in rules:
                    rule = rules[param]
                    
                    # Type validation
                    if 'type' in rule and not isinstance(value, rule['type']):
                        errors.append(f"{param} must be of type {rule['type'].__name__}")
                    
                    # Range validation
                    if 'min' in rule and value < rule['min']:
                        errors.append(f"{param} must be >= {rule['min']}")
                    
                    if 'max' in rule and value > rule['max']:
                        errors.append(f"{param} must be <= {rule['max']}")
                    
                    # Value validation
                    if 'values' in rule and value not in rule['values']:
                        errors.append(f"{param} must be one of {rule['values']}")
            
            return {
                'valid': len(errors) == 0,
                'errors': errors
            }
            
        except Exception as e:
            logger.error(f"Error validating parameters for {strategy_id}: {e}")
            return {
                'valid': False,
                'errors': [f"Validation error: {str(e)}"]
            }
