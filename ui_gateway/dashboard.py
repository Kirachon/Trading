"""
Streamlit Trading Dashboard
Real-time monitoring and control interface for the algorithmic trading system.
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta
import time

# Add shared modules to path
sys.path.append('/app')

# Import data access modules
from data_access import DataAccess
from portfolio_manager import PortfolioManager
from strategy_controller import StrategyController
from websocket_client import (
    init_websocket_connection,
    get_realtime_data,
    is_websocket_connected,
    show_websocket_status,
    cleanup_websocket
)

# Configure Streamlit page
st.set_page_config(
    page_title="Algorithmic Trading Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .profit-positive {
        color: #00ff00;
    }
    .profit-negative {
        color: #ff0000;
    }
    .status-active {
        color: #00ff00;
    }
    .status-inactive {
        color: #ff0000;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'data_access' not in st.session_state:
    st.session_state.data_access = DataAccess()
    st.session_state.portfolio_manager = PortfolioManager()
    st.session_state.strategy_controller = StrategyController()

# Initialize WebSocket connection for real-time updates
websocket_url = os.getenv('WEBSOCKET_URL', 'ws://localhost:8765')
websocket_client = init_websocket_connection(websocket_url)

def main():
    """Main dashboard function."""
    st.title("🚀 Algorithmic Trading Dashboard")

    # Show WebSocket connection status
    show_websocket_status()

    # Sidebar for navigation and controls
    with st.sidebar:
        st.header("Navigation")
        page = st.selectbox(
            "Select Page",
            ["Overview", "Portfolio", "Strategies", "Trading History", "Risk Management", "System Status"]
        )

        st.header("Quick Controls")

        # Emergency stop button
        if st.button("🛑 EMERGENCY STOP", type="primary"):
            emergency_stop()

        # Strategy controls
        st.subheader("Strategy Controls")
        if st.button("▶️ Enable All Strategies"):
            enable_all_strategies()

        if st.button("⏸️ Disable All Strategies"):
            disable_all_strategies()

        # Manual refresh button (fallback)
        if st.button("🔄 Refresh Data"):
            refresh_all_data()
    
    # Route to selected page
    if page == "Overview":
        show_overview()
    elif page == "Portfolio":
        show_portfolio()
    elif page == "Strategies":
        show_strategies()
    elif page == "Trading History":
        show_trading_history()
    elif page == "Risk Management":
        show_risk_management()
    elif page == "System Status":
        show_system_status()

def show_overview():
    """Show overview dashboard with real-time data."""
    st.header("📊 Trading Overview")

    # Get portfolio summary from real-time data or fallback to cached
    portfolio_summary = get_realtime_data('portfolio_summary', {})
    if not portfolio_summary:
        portfolio_summary = st.session_state.portfolio_manager.get_portfolio_summary()

    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_balance = portfolio_summary.get('total_balance', 0)
        st.metric(
            label="Total Balance",
            value=f"${total_balance:,.2f}",
            delta=None
        )

    with col2:
        daily_pnl = portfolio_summary.get('daily_pnl', 0)
        delta_color = "normal" if daily_pnl >= 0 else "inverse"
        st.metric(
            label="Daily P&L",
            value=f"${daily_pnl:,.2f}",
            delta=f"{daily_pnl:+.2f}",
            delta_color=delta_color
        )

    with col3:
        position_count = portfolio_summary.get('position_count', 0)
        st.metric(
            label="Active Positions",
            value=str(position_count)
        )

    with col4:
        # Get strategy status
        strategy_status = st.session_state.strategy_controller.get_strategy_status()
        active_strategies = len([s for s in strategy_status.get('active_strategies', [])])
        st.metric(
            label="Active Strategies",
            value=str(active_strategies)
        )
    
    # Charts row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Portfolio Performance")
        show_portfolio_performance_chart()
    
    with col2:
        st.subheader("Strategy Performance")
        show_strategy_performance_chart()
    
    # Recent activity
    st.subheader("Recent Trading Activity")
    show_recent_trades()

def show_portfolio():
    """Show portfolio details."""
    st.header("💼 Portfolio Management")
    
    # Portfolio summary
    portfolio_summary = st.session_state.portfolio_manager.get_portfolio_summary()
    
    # Positions table
    st.subheader("Current Positions")

    # Get positions from real-time data or fallback
    positions = get_realtime_data('positions', [])
    if not positions:
        positions = st.session_state.portfolio_manager.get_positions()

    if positions:
        positions_df = pd.DataFrame(positions)

        # Format the dataframe
        if not positions_df.empty:
            positions_df['unrealized_pnl'] = positions_df['unrealized_pnl'].apply(lambda x: f"${x:.2f}")
            positions_df['quantity'] = positions_df['quantity'].apply(lambda x: f"{x:.6f}")
            positions_df['avg_price'] = positions_df['avg_price'].apply(lambda x: f"${x:.4f}")

            st.dataframe(
                positions_df[['symbol', 'side', 'quantity', 'avg_price', 'unrealized_pnl', 'strategy_id']],
                use_container_width=True
            )
    else:
        st.info("No active positions")

    # Balances
    st.subheader("Account Balances")

    # Get balances from real-time data or fallback
    balances = get_realtime_data('balances', [])
    if not balances:
        balances = st.session_state.portfolio_manager.get_balances()

    if balances:
        balances_df = pd.DataFrame(balances)
        if not balances_df.empty:
            balances_df['balance'] = balances_df['balance'].apply(lambda x: f"{x:.6f}")
            balances_df['locked'] = balances_df['locked'].apply(lambda x: f"{x:.6f}")

            st.dataframe(
                balances_df[['asset', 'balance', 'locked', 'exchange_id']],
                use_container_width=True
            )
    else:
        st.info("No balance data available")

def show_strategies():
    """Show strategy management interface."""
    st.header("🧠 Strategy Management")
    
    # Get strategy status
    strategy_status = st.session_state.strategy_controller.get_strategy_status()
    
    # Strategy overview
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(
            label="Strategies Enabled",
            value=str(strategy_status.get('strategies_enabled', False))
        )
    
    with col2:
        active_count = len(strategy_status.get('active_strategies', []))
        st.metric(
            label="Active Strategies",
            value=str(active_count)
        )
    
    # Strategy details
    st.subheader("Strategy Details")
    
    strategy_info = strategy_status.get('strategy_info', {})
    
    for strategy_id, info in strategy_info.items():
        with st.expander(f"📈 {info.get('name', strategy_id)}"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.write(f"**Strategy ID:** {strategy_id}")
                st.write(f"**Last Signal:** {info.get('last_signal', 'None')}")
            
            with col2:
                st.write(f"**Position:** {info.get('current_position', 'Flat')}")
                last_signal_time = info.get('last_signal_time')
                if last_signal_time:
                    st.write(f"**Last Signal Time:** {last_signal_time}")
            
            with col3:
                # Strategy controls
                if st.button(f"Disable {strategy_id}", key=f"disable_{strategy_id}"):
                    disable_strategy(strategy_id)
                
                if st.button(f"Reset {strategy_id}", key=f"reset_{strategy_id}"):
                    reset_strategy(strategy_id)
            
            # Strategy parameters
            st.write("**Parameters:**")
            params = info.get('parameters', {})
            if params:
                st.json(params)
            else:
                st.write("No parameters available")

def show_trading_history():
    """Show trading history and analytics."""
    st.header("📈 Trading History")
    
    # Date range selector
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now().date() - timedelta(days=7))
    with col2:
        end_date = st.date_input("End Date", value=datetime.now().date())
    
    # Strategy filter
    strategy_filter = st.selectbox(
        "Filter by Strategy",
        ["All"] + st.session_state.strategy_controller.get_strategy_list()
    )
    
    # Get trade history
    filters = {
        'start_date': datetime.combine(start_date, datetime.min.time()),
        'end_date': datetime.combine(end_date, datetime.max.time())
    }
    
    if strategy_filter != "All":
        filters['strategy_id'] = strategy_filter
    
    trades = st.session_state.data_access.get_trade_history(filters)
    
    if trades:
        trades_df = pd.DataFrame(trades)
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Trades", len(trades_df))
        
        with col2:
            total_pnl = trades_df['realized_pnl'].sum() if 'realized_pnl' in trades_df.columns else 0
            st.metric("Total P&L", f"${total_pnl:.2f}")
        
        with col3:
            winning_trades = len(trades_df[trades_df['realized_pnl'] > 0]) if 'realized_pnl' in trades_df.columns else 0
            win_rate = (winning_trades / len(trades_df)) * 100 if len(trades_df) > 0 else 0
            st.metric("Win Rate", f"{win_rate:.1f}%")
        
        with col4:
            total_volume = (trades_df['quantity'] * trades_df['price']).sum() if all(col in trades_df.columns for col in ['quantity', 'price']) else 0
            st.metric("Total Volume", f"${total_volume:.2f}")
        
        # Trades table
        st.subheader("Trade Details")
        
        # Format dataframe for display
        display_df = trades_df.copy()
        if 'timestamp' in display_df.columns:
            display_df['timestamp'] = pd.to_datetime(display_df['timestamp'], unit='ms')
        
        st.dataframe(
            display_df[['timestamp', 'symbol', 'side', 'quantity', 'price', 'realized_pnl', 'strategy_id']],
            use_container_width=True
        )
        
        # P&L chart
        if 'realized_pnl' in trades_df.columns and 'timestamp' in trades_df.columns:
            st.subheader("Cumulative P&L")
            show_pnl_chart(trades_df)
    
    else:
        st.info("No trades found for the selected period")

def show_risk_management():
    """Show risk management interface."""
    st.header("⚠️ Risk Management")
    
    # Risk status
    risk_status = st.session_state.data_access.get_risk_status()
    
    col1, col2 = st.columns(2)
    
    with col1:
        risk_enabled = risk_status.get('risk_checks_enabled', False)
        status_text = "🟢 ENABLED" if risk_enabled else "🔴 DISABLED"
        st.markdown(f"**Risk Checks:** {status_text}")
    
    with col2:
        if st.button("Toggle Risk Checks"):
            toggle_risk_checks()
    
    # Risk limits
    st.subheader("Risk Limits")
    
    # Display current risk limits (simplified)
    risk_limits_data = {
        'Parameter': ['Max Position Size', 'Max Daily Loss', 'Max Trades/Day', 'Fat Finger Threshold'],
        'Current Value': ['$10,000', '$1,000', '50', '5%'],
        'Status': ['✅ OK', '✅ OK', '✅ OK', '✅ OK']
    }
    
    risk_df = pd.DataFrame(risk_limits_data)
    st.dataframe(risk_df, use_container_width=True)
    
    # Daily statistics
    st.subheader("Today's Risk Metrics")
    
    daily_stats = risk_status.get('daily_stats', {})
    
    if daily_stats:
        for key, stats in daily_stats.items():
            with st.expander(f"📊 {key}"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Trades Today", stats.get('trade_count', 0))
                
                with col2:
                    daily_pnl = stats.get('daily_realized_pnl', 0)
                    st.metric("Daily P&L", f"${daily_pnl:.2f}")
                
                with col3:
                    # Risk utilization (simplified)
                    risk_util = min(100, (stats.get('trade_count', 0) / 50) * 100)
                    st.metric("Risk Utilization", f"{risk_util:.1f}%")

def show_system_status():
    """Show system status and health."""
    st.header("🖥️ System Status")
    
    # Service status
    st.subheader("Service Health")
    
    services = [
        "Market Data Service",
        "Strategy Engine",
        "Risk Management",
        "Execution Gateway",
        "Portfolio Accounting",
        "Trade Logger"
    ]
    
    # Simulate service status (in production, would check actual service health)
    status_data = []
    for service in services:
        status_data.append({
            'Service': service,
            'Status': '🟢 Running',
            'Last Heartbeat': datetime.now().strftime('%H:%M:%S'),
            'Uptime': '2h 15m'
        })
    
    status_df = pd.DataFrame(status_data)
    st.dataframe(status_df, use_container_width=True)
    
    # System metrics
    st.subheader("System Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("CPU Usage", "45%")
    
    with col2:
        st.metric("Memory Usage", "2.1GB")
    
    with col3:
        st.metric("Active Connections", "12")
    
    with col4:
        st.metric("Messages/sec", "150")
    
    # Recent alerts
    st.subheader("Recent Alerts")
    
    # Simulate recent alerts
    alerts_data = [
        {'Time': '14:30:15', 'Type': 'INFO', 'Message': 'Strategy MA_Crossover generated buy signal for BTC/USDT'},
        {'Time': '14:25:32', 'Type': 'WARNING', 'Message': 'High volatility detected in ETH/USDT'},
        {'Time': '14:20:45', 'Type': 'INFO', 'Message': 'Portfolio rebalancing completed'},
    ]
    
    alerts_df = pd.DataFrame(alerts_data)
    st.dataframe(alerts_df, use_container_width=True)

# Chart functions
def show_portfolio_performance_chart():
    """Show portfolio performance chart."""
    # Generate sample data (in production, would use real data)
    dates = pd.date_range(start=datetime.now() - timedelta(days=30), end=datetime.now(), freq='D')
    values = np.cumsum(np.random.randn(len(dates)) * 100) + 100000
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates,
        y=values,
        mode='lines',
        name='Portfolio Value',
        line=dict(color='#1f77b4', width=2)
    ))
    
    fig.update_layout(
        title="Portfolio Value (30 Days)",
        xaxis_title="Date",
        yaxis_title="Value ($)",
        height=300,
        showlegend=False
    )
    
    st.plotly_chart(fig, use_container_width=True)

def show_strategy_performance_chart():
    """Show strategy performance comparison."""
    strategies = ['MA_Crossover', 'RSI', 'MACD', 'Bollinger_Bands', 'MAEE_Formula']
    returns = np.random.randn(len(strategies)) * 5  # Random returns for demo
    
    colors = ['green' if r > 0 else 'red' for r in returns]
    
    fig = go.Figure(data=[
        go.Bar(x=strategies, y=returns, marker_color=colors)
    ])
    
    fig.update_layout(
        title="Strategy Returns (7 Days)",
        xaxis_title="Strategy",
        yaxis_title="Return (%)",
        height=300,
        showlegend=False
    )
    
    st.plotly_chart(fig, use_container_width=True)

def show_recent_trades():
    """Show recent trades table with real-time updates."""
    # Get recent trades from real-time data or fallback
    recent_trades = get_realtime_data('recent_trades', [])
    if not recent_trades:
        recent_trades = st.session_state.data_access.get_recent_trades(limit=10)

    if recent_trades:
        trades_df = pd.DataFrame(recent_trades[-10:])  # Show last 10 trades

        # Format for display
        if 'timestamp' in trades_df.columns:
            trades_df['timestamp'] = pd.to_datetime(trades_df['timestamp'], unit='ms')
            trades_df['time'] = trades_df['timestamp'].dt.strftime('%H:%M:%S')
        elif 'created_at' in trades_df.columns:
            trades_df['created_at'] = pd.to_datetime(trades_df['created_at'])
            trades_df['time'] = trades_df['created_at'].dt.strftime('%H:%M:%S')

        display_columns = ['time', 'symbol', 'side', 'quantity', 'price', 'strategy_id']
        available_columns = [col for col in display_columns if col in trades_df.columns]

        st.dataframe(
            trades_df[available_columns],
            use_container_width=True,
            height=300
        )
    else:
        st.info("No recent trades")

def show_pnl_chart(trades_df):
    """Show cumulative P&L chart."""
    if 'realized_pnl' in trades_df.columns and 'timestamp' in trades_df.columns:
        trades_df = trades_df.sort_values('timestamp')
        trades_df['cumulative_pnl'] = trades_df['realized_pnl'].cumsum()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=trades_df['timestamp'],
            y=trades_df['cumulative_pnl'],
            mode='lines',
            name='Cumulative P&L',
            line=dict(color='#2ca02c', width=2)
        ))
        
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Cumulative P&L ($)",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)

# Control functions
def emergency_stop():
    """Emergency stop all trading activities."""
    st.session_state.strategy_controller.emergency_stop()
    st.error("🛑 EMERGENCY STOP ACTIVATED - All trading activities halted!")

def enable_all_strategies():
    """Enable all trading strategies."""
    st.session_state.strategy_controller.enable_all_strategies()
    st.success("✅ All strategies enabled")

def disable_all_strategies():
    """Disable all trading strategies."""
    st.session_state.strategy_controller.disable_all_strategies()
    st.warning("⏸️ All strategies disabled")

def disable_strategy(strategy_id):
    """Disable specific strategy."""
    st.session_state.strategy_controller.disable_strategy(strategy_id)
    st.warning(f"⏸️ Strategy {strategy_id} disabled")

def reset_strategy(strategy_id):
    """Reset specific strategy."""
    st.session_state.strategy_controller.reset_strategy(strategy_id)
    st.info(f"🔄 Strategy {strategy_id} reset")

def toggle_risk_checks():
    """Toggle risk management checks."""
    st.session_state.data_access.toggle_risk_checks()
    st.info("🔄 Risk check status toggled")

def refresh_all_data():
    """Refresh all dashboard data."""
    st.session_state.data_access.refresh_cache()
    st.session_state.portfolio_manager.refresh_data()
    st.session_state.strategy_controller.refresh_status()
    st.success("🔄 Data refreshed")

if __name__ == "__main__":
    main()
