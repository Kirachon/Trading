"""
Portfolio Manager for Streamlit Dashboard
Handles portfolio-related operations and data.
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

class PortfolioManager:
    """Portfolio management interface for dashboard."""
    
    def __init__(self):
        self.data_access = DataAccess()
        
        # Cache for portfolio data
        self.cache = {
            'summary': {},
            'positions': [],
            'balances': [],
            'performance': {},
            'last_refresh': None
        }
        
        self.cache_ttl = 30  # Cache TTL in seconds
    
    def is_cache_valid(self) -> bool:
        """Check if cache is still valid."""
        if not self.cache['last_refresh']:
            return False
        
        elapsed = (datetime.now() - self.cache['last_refresh']).total_seconds()
        return elapsed < self.cache_ttl
    
    def refresh_data(self):
        """Force refresh of portfolio data."""
        self.cache['last_refresh'] = None
        self.data_access.refresh_cache()
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get portfolio summary."""
        if self.is_cache_valid() and self.cache['summary']:
            return self.cache['summary']
        
        try:
            summary = self.data_access.get_portfolio_summary()
            
            # Add additional calculated metrics
            positions = self.get_positions()
            
            # Calculate total unrealized P&L
            total_unrealized_pnl = sum(
                float(pos.get('unrealized_pnl', 0)) for pos in positions
            )
            
            # Calculate portfolio allocation
            balances = self.get_balances()
            total_value = summary.get('total_balance', 0) + total_unrealized_pnl
            
            allocation = {}
            for balance in balances:
                asset = balance['asset']
                value = float(balance['balance'])
                if total_value > 0:
                    allocation[asset] = (value / total_value) * 100
            
            enhanced_summary = {
                **summary,
                'total_unrealized_pnl': total_unrealized_pnl,
                'total_portfolio_value': total_value,
                'asset_allocation': allocation,
                'positions_count': len(positions),
                'active_strategies': len(set(pos.get('strategy_id') for pos in positions))
            }
            
            self.cache['summary'] = enhanced_summary
            self.cache['last_refresh'] = datetime.now()
            
            return enhanced_summary
            
        except Exception as e:
            logger.error(f"Error getting portfolio summary: {e}")
            return {}
    
    def get_positions(self) -> List[Dict[str, Any]]:
        """Get current positions with additional calculations."""
        if self.is_cache_valid() and self.cache['positions']:
            return self.cache['positions']
        
        try:
            positions = self.data_access.get_positions()
            
            # Enhance positions with additional data
            enhanced_positions = []
            for pos in positions:
                enhanced_pos = dict(pos)
                
                # Calculate position value
                quantity = float(pos.get('quantity', 0))
                avg_price = float(pos.get('avg_price', 0))
                position_value = quantity * avg_price
                enhanced_pos['position_value'] = position_value
                
                # Calculate percentage allocation (simplified)
                # In production, would use current market prices
                enhanced_pos['allocation_pct'] = 0.0  # Would calculate based on total portfolio
                
                # Format dates
                if 'opened_at' in pos and pos['opened_at']:
                    enhanced_pos['opened_at_formatted'] = pos['opened_at'].strftime('%Y-%m-%d %H:%M')
                
                # Calculate days held
                if 'opened_at' in pos and pos['opened_at']:
                    days_held = (datetime.now() - pos['opened_at']).days
                    enhanced_pos['days_held'] = days_held
                
                enhanced_positions.append(enhanced_pos)
            
            self.cache['positions'] = enhanced_positions
            return enhanced_positions
            
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []
    
    def get_balances(self) -> List[Dict[str, Any]]:
        """Get account balances with additional formatting."""
        if self.is_cache_valid() and self.cache['balances']:
            return self.cache['balances']
        
        try:
            balances = self.data_access.get_balances()
            
            # Enhance balances with additional data
            enhanced_balances = []
            for balance in balances:
                enhanced_balance = dict(balance)
                
                # Calculate total (available + locked)
                available = float(balance.get('balance', 0))
                locked = float(balance.get('locked', 0))
                total = available + locked
                enhanced_balance['total'] = total
                
                # Calculate locked percentage
                if total > 0:
                    locked_pct = (locked / total) * 100
                    enhanced_balance['locked_pct'] = locked_pct
                else:
                    enhanced_balance['locked_pct'] = 0.0
                
                # Format update time
                if 'updated_at' in balance and balance['updated_at']:
                    enhanced_balance['updated_at_formatted'] = balance['updated_at'].strftime('%Y-%m-%d %H:%M')
                
                enhanced_balances.append(enhanced_balance)
            
            # Sort by total value (descending)
            enhanced_balances.sort(key=lambda x: x['total'], reverse=True)
            
            self.cache['balances'] = enhanced_balances
            return enhanced_balances
            
        except Exception as e:
            logger.error(f"Error getting balances: {e}")
            return []
    
    def get_performance_metrics(self, days: int = 30) -> Dict[str, Any]:
        """Get portfolio performance metrics."""
        cache_key = f'performance_{days}'
        if self.is_cache_valid() and cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            # Get trade history for the period
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            filters = {
                'start_date': start_date,
                'end_date': end_date
            }
            
            trades = self.data_access.get_trade_history(filters)
            
            if not trades:
                return {'no_data': True, 'period_days': days}
            
            # Calculate performance metrics
            total_trades = len(trades)
            total_pnl = sum(float(trade.get('realized_pnl', 0)) for trade in trades)
            
            # Calculate win/loss statistics
            winning_trades = [t for t in trades if float(t.get('realized_pnl', 0)) > 0]
            losing_trades = [t for t in trades if float(t.get('realized_pnl', 0)) < 0]
            
            win_count = len(winning_trades)
            loss_count = len(losing_trades)
            win_rate = (win_count / total_trades) * 100 if total_trades > 0 else 0
            
            # Calculate average win/loss
            avg_win = sum(float(t.get('realized_pnl', 0)) for t in winning_trades) / win_count if win_count > 0 else 0
            avg_loss = sum(float(t.get('realized_pnl', 0)) for t in losing_trades) / loss_count if loss_count > 0 else 0
            
            # Calculate profit factor
            total_wins = sum(float(t.get('realized_pnl', 0)) for t in winning_trades)
            total_losses = abs(sum(float(t.get('realized_pnl', 0)) for t in losing_trades))
            profit_factor = total_wins / total_losses if total_losses > 0 else float('inf')
            
            # Calculate maximum drawdown (simplified)
            cumulative_pnl = []
            running_total = 0
            for trade in sorted(trades, key=lambda x: x.get('created_at', datetime.min)):
                running_total += float(trade.get('realized_pnl', 0))
                cumulative_pnl.append(running_total)
            
            max_drawdown = 0
            if cumulative_pnl:
                peak = cumulative_pnl[0]
                for value in cumulative_pnl:
                    if value > peak:
                        peak = value
                    drawdown = peak - value
                    if drawdown > max_drawdown:
                        max_drawdown = drawdown
            
            # Calculate Sharpe ratio (simplified)
            if len(cumulative_pnl) > 1:
                returns = [cumulative_pnl[i] - cumulative_pnl[i-1] for i in range(1, len(cumulative_pnl))]
                avg_return = sum(returns) / len(returns)
                return_std = (sum((r - avg_return) ** 2 for r in returns) / len(returns)) ** 0.5
                sharpe_ratio = avg_return / return_std if return_std > 0 else 0
            else:
                sharpe_ratio = 0
            
            # Calculate total volume
            total_volume = sum(
                float(trade.get('quantity', 0)) * float(trade.get('price', 0)) 
                for trade in trades
            )
            
            # Calculate commission paid
            total_commission = sum(float(trade.get('commission', 0)) for trade in trades)
            
            metrics = {
                'period_days': days,
                'total_trades': total_trades,
                'winning_trades': win_count,
                'losing_trades': loss_count,
                'win_rate': win_rate,
                'total_pnl': total_pnl,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'profit_factor': profit_factor,
                'max_drawdown': max_drawdown,
                'sharpe_ratio': sharpe_ratio,
                'total_volume': total_volume,
                'total_commission': total_commission,
                'avg_trades_per_day': total_trades / days if days > 0 else 0,
                'pnl_per_trade': total_pnl / total_trades if total_trades > 0 else 0
            }
            
            self.cache[cache_key] = metrics
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {e}")
            return {'error': str(e), 'period_days': days}
    
    def get_position_by_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get position for a specific symbol."""
        positions = self.get_positions()
        for pos in positions:
            if pos.get('symbol') == symbol:
                return pos
        return None
    
    def get_balance_by_asset(self, asset: str) -> Optional[Dict[str, Any]]:
        """Get balance for a specific asset."""
        balances = self.get_balances()
        for balance in balances:
            if balance.get('asset') == asset:
                return balance
        return None
    
    def get_strategy_positions(self, strategy_id: str) -> List[Dict[str, Any]]:
        """Get all positions for a specific strategy."""
        positions = self.get_positions()
        return [pos for pos in positions if pos.get('strategy_id') == strategy_id]
    
    def calculate_portfolio_risk(self) -> Dict[str, Any]:
        """Calculate portfolio risk metrics."""
        try:
            positions = self.get_positions()
            summary = self.get_portfolio_summary()
            
            total_value = summary.get('total_portfolio_value', 0)
            
            if total_value == 0:
                return {'no_positions': True}
            
            # Calculate concentration risk
            position_concentrations = []
            for pos in positions:
                position_value = float(pos.get('position_value', 0))
                concentration = (position_value / total_value) * 100
                position_concentrations.append({
                    'symbol': pos.get('symbol'),
                    'concentration_pct': concentration
                })
            
            # Find largest concentration
            max_concentration = max(position_concentrations, key=lambda x: x['concentration_pct']) if position_concentrations else None
            
            # Calculate strategy diversification
            strategy_counts = {}
            for pos in positions:
                strategy = pos.get('strategy_id', 'unknown')
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
            
            # Calculate exchange diversification
            exchange_counts = {}
            for pos in positions:
                exchange = pos.get('exchange_id', 'unknown')
                exchange_counts[exchange] = exchange_counts.get(exchange, 0) + 1
            
            risk_metrics = {
                'total_positions': len(positions),
                'max_concentration': max_concentration,
                'strategy_diversification': len(strategy_counts),
                'exchange_diversification': len(exchange_counts),
                'position_concentrations': position_concentrations,
                'strategy_distribution': strategy_counts,
                'exchange_distribution': exchange_counts
            }
            
            return risk_metrics
            
        except Exception as e:
            logger.error(f"Error calculating portfolio risk: {e}")
            return {'error': str(e)}
    
    def send_portfolio_command(self, command_type: str, **kwargs):
        """Send command to portfolio service."""
        try:
            command = {
                'type': command_type,
                **kwargs
            }
            self.data_access.send_command('portfolio', command)
            
        except Exception as e:
            logger.error(f"Error sending portfolio command: {e}")
    
    def request_portfolio_refresh(self):
        """Request portfolio data refresh."""
        self.send_portfolio_command('refresh_portfolio')
        self.refresh_data()
    
    def get_portfolio_summary_for_display(self) -> Dict[str, str]:
        """Get formatted portfolio summary for display."""
        summary = self.get_portfolio_summary()
        
        return {
            'Total Balance': f"${summary.get('total_balance', 0):,.2f}",
            'Daily P&L': f"${summary.get('daily_pnl', 0):+,.2f}",
            'Unrealized P&L': f"${summary.get('total_unrealized_pnl', 0):+,.2f}",
            'Total Value': f"${summary.get('total_portfolio_value', 0):,.2f}",
            'Active Positions': str(summary.get('positions_count', 0)),
            'Active Strategies': str(summary.get('active_strategies', 0))
        }
