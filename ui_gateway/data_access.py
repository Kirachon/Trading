"""
Data Access Layer for Streamlit Dashboard
Handles database connections and data retrieval.
"""
import os
import sys
import json
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import pymongo
from pymongo import MongoClient
import pandas as pd
import redis
import logging

# Add shared modules to path
sys.path.append('/app')
from shared.messaging import RabbitMQPublisher

logger = logging.getLogger(__name__)

class DataAccess:
    """Data access layer for dashboard."""
    
    def __init__(self):
        self.postgres_url = os.getenv('POSTGRES_URL', 'postgresql://admin:SecureDB2024!@postgresql:5432/trading')
        self.mongodb_url = os.getenv('MONGODB_URL', 'mongodb://admin:SecureDB2024!@mongodb:27017/trading_data?authSource=admin')
        self.redis_url = os.getenv('REDIS_URL', 'redis://redis:6379')
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:SecureRabbit2024!@rabbitmq:5672/')
        
        # Database connections
        self.postgres_conn = None
        self.mongo_client = None
        self.redis_client = None
        self.rabbitmq_publisher = None
        
        # Data cache
        self.cache = {
            'last_refresh': None,
            'portfolio_summary': {},
            'positions': [],
            'balances': [],
            'recent_trades': [],
            'strategy_status': {},
            'risk_status': {}
        }
        
        self.cache_ttl = 30  # Cache TTL in seconds
        
        # Initialize connections
        self.connect_databases()
    
    def connect_databases(self):
        """Initialize database connections."""
        try:
            # PostgreSQL connection
            self.postgres_conn = psycopg2.connect(self.postgres_url)
            logger.info("Connected to PostgreSQL")
            
            # MongoDB connection
            self.mongo_client = MongoClient(self.mongodb_url)
            logger.info("Connected to MongoDB")
            
            # Redis connection
            self.redis_client = redis.from_url(self.redis_url)
            logger.info("Connected to Redis")
            
        except Exception as e:
            logger.error(f"Error connecting to databases: {e}")
    
    def is_cache_valid(self) -> bool:
        """Check if cache is still valid."""
        if not self.cache['last_refresh']:
            return False
        
        elapsed = (datetime.now() - self.cache['last_refresh']).total_seconds()
        return elapsed < self.cache_ttl
    
    def refresh_cache(self):
        """Force refresh of all cached data."""
        self.cache['last_refresh'] = None
        self.get_portfolio_summary()
        self.get_positions()
        self.get_balances()
        self.get_recent_trades()
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get portfolio summary with caching."""
        if self.is_cache_valid() and self.cache['portfolio_summary']:
            return self.cache['portfolio_summary']
        
        try:
            cursor = self.postgres_conn.cursor(cursor_factory=RealDictCursor)
            
            # Get total balance
            cursor.execute("""
                SELECT SUM(balance) as total_balance
                FROM portfolio 
                WHERE asset = 'USDT'
            """)
            balance_result = cursor.fetchone()
            total_balance = float(balance_result['total_balance'] or 0)
            
            # Get today's P&L
            today = datetime.now().date()
            cursor.execute("""
                SELECT SUM(realized_pnl) as daily_pnl
                FROM trades 
                WHERE DATE(created_at) = %s AND status = 'filled'
            """, (today,))
            pnl_result = cursor.fetchone()
            daily_pnl = float(pnl_result['daily_pnl'] or 0)
            
            # Get position count
            cursor.execute("SELECT COUNT(*) as position_count FROM positions")
            pos_result = cursor.fetchone()
            position_count = int(pos_result['position_count'] or 0)
            
            cursor.close()
            
            summary = {
                'total_balance': total_balance,
                'daily_pnl': daily_pnl,
                'position_count': position_count,
                'last_update': datetime.now().isoformat()
            }
            
            self.cache['portfolio_summary'] = summary
            self.cache['last_refresh'] = datetime.now()
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting portfolio summary: {e}")
            return {
                'total_balance': 0.0,
                'daily_pnl': 0.0,
                'position_count': 0,
                'last_update': datetime.now().isoformat()
            }
    
    def get_positions(self) -> List[Dict[str, Any]]:
        """Get current positions."""
        if self.is_cache_valid() and self.cache['positions']:
            return self.cache['positions']
        
        try:
            cursor = self.postgres_conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT exchange_id, symbol, side, quantity, avg_price, 
                       unrealized_pnl, realized_pnl, strategy_id, opened_at
                FROM positions
                ORDER BY opened_at DESC
            """)
            
            positions = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            
            self.cache['positions'] = positions
            return positions
            
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []
    
    def get_balances(self) -> List[Dict[str, Any]]:
        """Get account balances."""
        if self.is_cache_valid() and self.cache['balances']:
            return self.cache['balances']
        
        try:
            cursor = self.postgres_conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT exchange_id, asset, balance, locked, updated_at
                FROM portfolio
                WHERE balance > 0 OR locked > 0
                ORDER BY balance DESC
            """)
            
            balances = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            
            self.cache['balances'] = balances
            return balances
            
        except Exception as e:
            logger.error(f"Error getting balances: {e}")
            return []
    
    def get_recent_trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent trades."""
        if self.is_cache_valid() and self.cache['recent_trades']:
            return self.cache['recent_trades'][:limit]
        
        try:
            cursor = self.postgres_conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT exchange_id, symbol, order_id, side, quantity, price, 
                       timestamp, strategy_id, status, commission, realized_pnl, created_at
                FROM trades
                WHERE status = 'filled'
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            
            trades = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            
            self.cache['recent_trades'] = trades
            return trades
            
        except Exception as e:
            logger.error(f"Error getting recent trades: {e}")
            return []
    
    def get_trade_history(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get trade history with filters."""
        try:
            cursor = self.postgres_conn.cursor(cursor_factory=RealDictCursor)
            
            # Build query with filters
            query = """
                SELECT exchange_id, symbol, order_id, side, quantity, price, 
                       timestamp, strategy_id, status, commission, realized_pnl, created_at
                FROM trades
                WHERE status = 'filled'
            """
            params = []
            
            if 'start_date' in filters:
                query += " AND created_at >= %s"
                params.append(filters['start_date'])
            
            if 'end_date' in filters:
                query += " AND created_at <= %s"
                params.append(filters['end_date'])
            
            if 'strategy_id' in filters:
                query += " AND strategy_id = %s"
                params.append(filters['strategy_id'])
            
            if 'symbol' in filters:
                query += " AND symbol = %s"
                params.append(filters['symbol'])
            
            query += " ORDER BY created_at DESC LIMIT 1000"
            
            cursor.execute(query, params)
            trades = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            
            return trades
            
        except Exception as e:
            logger.error(f"Error getting trade history: {e}")
            return []
    
    def get_strategy_performance(self, strategy_id: str, days: int = 30) -> Dict[str, Any]:
        """Get strategy performance metrics."""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            cursor = self.postgres_conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                    SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                    SUM(realized_pnl) as total_pnl,
                    AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl END) as avg_win,
                    AVG(CASE WHEN realized_pnl < 0 THEN realized_pnl END) as avg_loss,
                    MAX(realized_pnl) as max_win,
                    MIN(realized_pnl) as max_loss
                FROM trades 
                WHERE strategy_id = %s 
                AND created_at >= %s 
                AND created_at <= %s 
                AND status = 'filled'
            """, (strategy_id, start_date, end_date))
            
            result = cursor.fetchone()
            cursor.close()
            
            if result and result['total_trades'] > 0:
                win_rate = float(result['winning_trades'] or 0) / float(result['total_trades'])
                
                return {
                    'strategy_id': strategy_id,
                    'period_days': days,
                    'total_trades': result['total_trades'],
                    'winning_trades': result['winning_trades'],
                    'losing_trades': result['losing_trades'],
                    'win_rate': win_rate,
                    'total_pnl': float(result['total_pnl'] or 0),
                    'avg_win': float(result['avg_win'] or 0),
                    'avg_loss': float(result['avg_loss'] or 0),
                    'max_win': float(result['max_win'] or 0),
                    'max_loss': float(result['max_loss'] or 0)
                }
            else:
                return {'strategy_id': strategy_id, 'no_data': True}
                
        except Exception as e:
            logger.error(f"Error getting strategy performance: {e}")
            return {'error': str(e)}
    
    def get_risk_status(self) -> Dict[str, Any]:
        """Get risk management status."""
        # Simplified risk status - in production would query actual risk service
        return {
            'risk_checks_enabled': True,
            'risk_limits_count': 5,
            'active_positions': len(self.get_positions()),
            'daily_stats': {
                'binance:BTC/USDT:ma_crossover': {
                    'trade_count': 3,
                    'daily_realized_pnl': 25.50
                },
                'binance:ETH/USDT:rsi_strategy': {
                    'trade_count': 2,
                    'daily_realized_pnl': -12.30
                }
            }
        }
    
    def get_market_data(self, symbol: str, timeframe: str = '1h', limit: int = 100) -> List[Dict[str, Any]]:
        """Get market data from MongoDB."""
        try:
            db = self.mongo_client['trading_data']
            collection_name = f"binance_{symbol.replace('/', '_')}_{timeframe}"
            collection = db[collection_name]
            
            # Get recent data
            cursor = collection.find().sort('timestamp', -1).limit(limit)
            data = list(cursor)
            
            # Convert ObjectId to string and format data
            for item in data:
                item['_id'] = str(item['_id'])
                if 'datetime' in item:
                    item['datetime'] = item['datetime'].isoformat()
            
            return data[::-1]  # Reverse to get chronological order
            
        except Exception as e:
            logger.error(f"Error getting market data: {e}")
            return []
    
    def send_command(self, service: str, command: Dict[str, Any]):
        """Send command to a service via RabbitMQ."""
        try:
            if not self.rabbitmq_publisher:
                from shared.messaging import RabbitMQPublisher
                self.rabbitmq_publisher = RabbitMQPublisher(self.rabbitmq_url)
                asyncio.run(self.rabbitmq_publisher.connect())
            
            routing_key = f"commands.{service}"
            asyncio.run(self.rabbitmq_publisher.publish(command, routing_key))
            
        except Exception as e:
            logger.error(f"Error sending command: {e}")
    
    def toggle_risk_checks(self):
        """Toggle risk management checks."""
        command = {'type': 'toggle_risk_checks'}
        self.send_command('risk', command)
    
    def __del__(self):
        """Cleanup database connections."""
        try:
            if self.postgres_conn:
                self.postgres_conn.close()
            if self.mongo_client:
                self.mongo_client.close()
            if self.redis_client:
                self.redis_client.close()
        except:
            pass
