"""
WebSocket Server for Real-time Trading Data Streaming

This module provides WebSocket-based real-time data streaming to replace
the inefficient auto-refresh polling mechanism in the UI.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Set, Any, Optional
import websockets
from websockets.server import WebSocketServerProtocol
import redis.asyncio as redis_async
import asyncpg
from motor.motor_asyncio import AsyncIOMotorClient

# Add shared modules to path
sys.path.append('/app')
from shared.messaging import RedisSubscriber

logger = logging.getLogger(__name__)


class WebSocketDataStreamer:
    """WebSocket server for streaming real-time trading data."""
    
    def __init__(self):
        self.postgres_url = os.getenv('POSTGRES_URL', 'postgresql://admin:SecureDB2024!@postgresql:5432/trading')
        self.mongodb_url = os.getenv('MONGODB_URL', 'mongodb://admin:SecureDB2024!@mongodb:27017/trading_data?authSource=admin')
        self.redis_url = os.getenv('REDIS_URL', 'redis://redis:6379')
        
        # WebSocket connections
        self.connections: Set[WebSocketServerProtocol] = set()
        self.subscriptions: Dict[WebSocketServerProtocol, Set[str]] = {}
        
        # Database connections
        self.db_pool: Optional[asyncpg.Pool] = None
        self.mongo_client: Optional[AsyncIOMotorClient] = None
        self.redis_client: Optional[redis_async.Redis] = None
        self.redis_subscriber: Optional[RedisSubscriber] = None
        
        # Data cache for efficient streaming
        self.data_cache = {
            'portfolio_summary': {},
            'positions': [],
            'balances': [],
            'recent_trades': [],
            'market_data': {},
            'last_update': {}
        }
        
        # Update intervals (seconds)
        self.update_intervals = {
            'portfolio': 5,
            'positions': 10,
            'balances': 15,
            'trades': 5,
            'market_data': 1
        }
        
    async def initialize(self):
        """Initialize database connections and data streams."""
        try:
            # Connect to PostgreSQL
            self.db_pool = await asyncpg.create_pool(self.postgres_url)
            logger.info("Connected to PostgreSQL for WebSocket streaming")
            
            # Connect to MongoDB
            self.mongo_client = AsyncIOMotorClient(self.mongodb_url)
            logger.info("Connected to MongoDB for WebSocket streaming")
            
            # Connect to Redis
            self.redis_client = redis_async.from_url(self.redis_url)
            self.redis_subscriber = RedisSubscriber(self.redis_url)
            logger.info("Connected to Redis for WebSocket streaming")
            
            # Start background data update tasks
            asyncio.create_task(self.portfolio_data_updater())
            asyncio.create_task(self.market_data_streamer())
            
            logger.info("WebSocket data streamer initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize WebSocket streamer: {e}")
            raise
    
    async def register_connection(self, websocket: WebSocketServerProtocol):
        """Register a new WebSocket connection."""
        self.connections.add(websocket)
        self.subscriptions[websocket] = set()
        logger.info(f"New WebSocket connection registered: {websocket.remote_address}")
        
        # Send initial data
        await self.send_initial_data(websocket)
    
    async def unregister_connection(self, websocket: WebSocketServerProtocol):
        """Unregister a WebSocket connection."""
        self.connections.discard(websocket)
        self.subscriptions.pop(websocket, None)
        logger.info(f"WebSocket connection unregistered: {websocket.remote_address}")
    
    async def handle_subscription(self, websocket: WebSocketServerProtocol, message: Dict[str, Any]):
        """Handle subscription requests from clients."""
        try:
            action = message.get('action')
            data_type = message.get('type')
            
            if action == 'subscribe' and data_type:
                self.subscriptions[websocket].add(data_type)
                logger.debug(f"Client subscribed to {data_type}")
                
                # Send current data for this type
                await self.send_data_by_type(websocket, data_type)
                
            elif action == 'unsubscribe' and data_type:
                self.subscriptions[websocket].discard(data_type)
                logger.debug(f"Client unsubscribed from {data_type}")
                
        except Exception as e:
            logger.error(f"Error handling subscription: {e}")
    
    async def send_initial_data(self, websocket: WebSocketServerProtocol):
        """Send initial data to a new connection."""
        try:
            initial_data = {
                'type': 'initial',
                'data': {
                    'portfolio_summary': self.data_cache['portfolio_summary'],
                    'positions': self.data_cache['positions'],
                    'balances': self.data_cache['balances'],
                    'recent_trades': self.data_cache['recent_trades'][-10:],  # Last 10 trades
                },
                'timestamp': datetime.now().isoformat()
            }
            
            await websocket.send(json.dumps(initial_data))
            
        except Exception as e:
            logger.error(f"Error sending initial data: {e}")
    
    async def send_data_by_type(self, websocket: WebSocketServerProtocol, data_type: str):
        """Send specific data type to a connection."""
        try:
            data = self.data_cache.get(data_type, {})
            message = {
                'type': data_type,
                'data': data,
                'timestamp': datetime.now().isoformat()
            }
            
            await websocket.send(json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error sending {data_type} data: {e}")
    
    async def broadcast_update(self, data_type: str, data: Any):
        """Broadcast data update to all subscribed connections."""
        if not self.connections:
            return
        
        message = {
            'type': data_type,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        
        message_json = json.dumps(message)
        
        # Send to all connections subscribed to this data type
        disconnected = set()
        for websocket in self.connections:
            if data_type in self.subscriptions.get(websocket, set()):
                try:
                    await websocket.send(message_json)
                except websockets.exceptions.ConnectionClosed:
                    disconnected.add(websocket)
                except Exception as e:
                    logger.error(f"Error broadcasting to {websocket.remote_address}: {e}")
                    disconnected.add(websocket)
        
        # Clean up disconnected connections
        for websocket in disconnected:
            await self.unregister_connection(websocket)
    
    async def portfolio_data_updater(self):
        """Background task to update portfolio data."""
        while True:
            try:
                # Update portfolio summary
                portfolio_summary = await self.get_portfolio_summary()
                if portfolio_summary != self.data_cache['portfolio_summary']:
                    self.data_cache['portfolio_summary'] = portfolio_summary
                    await self.broadcast_update('portfolio_summary', portfolio_summary)
                
                # Update positions
                positions = await self.get_positions()
                if positions != self.data_cache['positions']:
                    self.data_cache['positions'] = positions
                    await self.broadcast_update('positions', positions)
                
                # Update balances
                balances = await self.get_balances()
                if balances != self.data_cache['balances']:
                    self.data_cache['balances'] = balances
                    await self.broadcast_update('balances', balances)
                
                # Update recent trades
                recent_trades = await self.get_recent_trades()
                if recent_trades != self.data_cache['recent_trades']:
                    self.data_cache['recent_trades'] = recent_trades
                    await self.broadcast_update('recent_trades', recent_trades)
                
                await asyncio.sleep(self.update_intervals['portfolio'])
                
            except Exception as e:
                logger.error(f"Error in portfolio data updater: {e}")
                await asyncio.sleep(5)  # Wait before retrying
    
    async def market_data_streamer(self):
        """Stream real-time market data from Redis."""
        try:
            await self.redis_subscriber.subscribe(
                ['market-data:*:ohlcv'],
                self.handle_market_data_update
            )
        except Exception as e:
            logger.error(f"Error in market data streamer: {e}")
    
    async def handle_market_data_update(self, data: Dict[str, Any], channel: str):
        """Handle real-time market data updates."""
        try:
            symbol = data.get('symbol')
            if symbol:
                self.data_cache['market_data'][symbol] = data
                await self.broadcast_update('market_data', {symbol: data})
                
        except Exception as e:
            logger.error(f"Error handling market data update: {e}")
    
    async def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get portfolio summary from database."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get total balance
                balance_result = await conn.fetchrow("""
                    SELECT SUM(balance) as total_balance
                    FROM portfolio 
                    WHERE asset = 'USDT'
                """)
                total_balance = float(balance_result['total_balance'] or 0)
                
                # Get today's P&L
                today = datetime.now().date()
                pnl_result = await conn.fetchrow("""
                    SELECT SUM(realized_pnl) as daily_pnl
                    FROM trades 
                    WHERE DATE(created_at) = $1 AND status = 'filled'
                """, today)
                daily_pnl = float(pnl_result['daily_pnl'] or 0)
                
                # Get position count
                pos_result = await conn.fetchrow("SELECT COUNT(*) as position_count FROM positions")
                position_count = int(pos_result['position_count'] or 0)
                
                return {
                    'total_balance': total_balance,
                    'daily_pnl': daily_pnl,
                    'position_count': position_count,
                    'last_updated': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error getting portfolio summary: {e}")
            return {}
    
    async def get_positions(self) -> list:
        """Get current positions from database."""
        try:
            async with self.db_pool.acquire() as conn:
                positions = await conn.fetch("""
                    SELECT exchange_id, symbol, side, quantity, avg_price,
                           unrealized_pnl, strategy_id, opened_at
                    FROM positions
                    ORDER BY opened_at DESC
                """)
                
                return [dict(pos) for pos in positions]
                
        except Exception as e:
            logger.error(f"Error getting positions: {e}")
            return []
    
    async def get_balances(self) -> list:
        """Get account balances from database."""
        try:
            async with self.db_pool.acquire() as conn:
                balances = await conn.fetch("""
                    SELECT exchange_id, asset, balance, locked, updated_at
                    FROM portfolio
                    WHERE balance > 0 OR locked > 0
                    ORDER BY balance DESC
                """)
                
                return [dict(balance) for balance in balances]
                
        except Exception as e:
            logger.error(f"Error getting balances: {e}")
            return []
    
    async def get_recent_trades(self) -> list:
        """Get recent trades from database."""
        try:
            async with self.db_pool.acquire() as conn:
                trades = await conn.fetch("""
                    SELECT exchange_id, symbol, side, quantity, price,
                           realized_pnl, strategy_id, created_at
                    FROM trades
                    WHERE status = 'filled'
                    ORDER BY created_at DESC
                    LIMIT 50
                """)
                
                return [dict(trade) for trade in trades]
                
        except Exception as e:
            logger.error(f"Error getting recent trades: {e}")
            return []
    
    async def cleanup(self):
        """Clean up resources."""
        try:
            if self.db_pool:
                await self.db_pool.close()
            if self.mongo_client:
                self.mongo_client.close()
            if self.redis_client:
                await self.redis_client.close()
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


async def websocket_handler(websocket: WebSocketServerProtocol, path: str):
    """Handle WebSocket connections."""
    streamer = websocket_streamer  # Global instance
    
    await streamer.register_connection(websocket)
    
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                await streamer.handle_subscription(websocket, data)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON received: {message}")
            except Exception as e:
                logger.error(f"Error handling WebSocket message: {e}")
                
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await streamer.unregister_connection(websocket)


# Global WebSocket streamer instance
websocket_streamer = WebSocketDataStreamer()


async def start_websocket_server(host: str = "0.0.0.0", port: int = 8765):
    """Start the WebSocket server."""
    await websocket_streamer.initialize()
    
    logger.info(f"Starting WebSocket server on {host}:{port}")
    
    async with websockets.serve(websocket_handler, host, port):
        logger.info("WebSocket server started successfully")
        await asyncio.Future()  # Run forever


if __name__ == "__main__":
    asyncio.run(start_websocket_server())
