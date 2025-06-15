"""
WebSocket Client for Streamlit Dashboard

This module provides WebSocket client functionality for receiving
real-time data updates in the Streamlit dashboard.
"""

import asyncio
import json
import logging
import threading
import time
from typing import Dict, Any, Callable, Optional
import websockets
from websockets.client import WebSocketClientProtocol
import streamlit as st

logger = logging.getLogger(__name__)


class StreamlitWebSocketClient:
    """WebSocket client for Streamlit dashboard real-time updates."""
    
    def __init__(self, websocket_url: str = "ws://localhost:8765"):
        self.websocket_url = websocket_url
        self.websocket: Optional[WebSocketClientProtocol] = None
        self.connected = False
        self.running = False
        
        # Data storage for Streamlit
        self.data_store = {
            'portfolio_summary': {},
            'positions': [],
            'balances': [],
            'recent_trades': [],
            'market_data': {},
            'last_update': {}
        }
        
        # Callbacks for data updates
        self.callbacks: Dict[str, list] = {}
        
        # Background thread for WebSocket connection
        self.thread: Optional[threading.Thread] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        
    def start(self):
        """Start the WebSocket client in a background thread."""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run_websocket_loop, daemon=True)
        self.thread.start()
        
        # Wait a moment for connection to establish
        time.sleep(1)
        
    def stop(self):
        """Stop the WebSocket client."""
        self.running = False
        if self.loop and not self.loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._disconnect(), self.loop)
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
    
    def _run_websocket_loop(self):
        """Run the WebSocket event loop in a separate thread."""
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._websocket_loop())
        except Exception as e:
            logger.error(f"Error in WebSocket loop: {e}")
        finally:
            if self.loop and not self.loop.is_closed():
                self.loop.close()
    
    async def _websocket_loop(self):
        """Main WebSocket connection loop."""
        while self.running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
                await asyncio.sleep(5)  # Wait before reconnecting
    
    async def _connect_and_listen(self):
        """Connect to WebSocket server and listen for messages."""
        try:
            async with websockets.connect(self.websocket_url) as websocket:
                self.websocket = websocket
                self.connected = True
                logger.info("Connected to WebSocket server")
                
                # Subscribe to all data types
                await self._subscribe_to_all()
                
                # Listen for messages
                async for message in websocket:
                    await self._handle_message(message)
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket connection closed")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            self.connected = False
            self.websocket = None
    
    async def _disconnect(self):
        """Disconnect from WebSocket server."""
        if self.websocket:
            await self.websocket.close()
    
    async def _subscribe_to_all(self):
        """Subscribe to all data types."""
        data_types = ['portfolio_summary', 'positions', 'balances', 'recent_trades', 'market_data']
        
        for data_type in data_types:
            await self._send_subscription(data_type, 'subscribe')
    
    async def _send_subscription(self, data_type: str, action: str):
        """Send subscription message to server."""
        if not self.websocket:
            return
        
        message = {
            'action': action,
            'type': data_type
        }
        
        try:
            await self.websocket.send(json.dumps(message))
        except Exception as e:
            logger.error(f"Error sending subscription: {e}")
    
    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)
            message_type = data.get('type')
            message_data = data.get('data', {})
            
            if message_type in self.data_store:
                # Update data store
                self.data_store[message_type] = message_data
                self.data_store['last_update'][message_type] = data.get('timestamp')
                
                # Trigger callbacks
                await self._trigger_callbacks(message_type, message_data)
                
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON received: {message}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    async def _trigger_callbacks(self, data_type: str, data: Any):
        """Trigger registered callbacks for data updates."""
        callbacks = self.callbacks.get(data_type, [])
        for callback in callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
            except Exception as e:
                logger.error(f"Error in callback for {data_type}: {e}")
    
    def register_callback(self, data_type: str, callback: Callable):
        """Register a callback for data updates."""
        if data_type not in self.callbacks:
            self.callbacks[data_type] = []
        self.callbacks[data_type].append(callback)
    
    def get_data(self, data_type: str) -> Any:
        """Get current data for a specific type."""
        return self.data_store.get(data_type, {})
    
    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self.connected
    
    def get_last_update(self, data_type: str) -> Optional[str]:
        """Get timestamp of last update for a data type."""
        return self.data_store['last_update'].get(data_type)


class StreamlitWebSocketManager:
    """Singleton manager for WebSocket client in Streamlit."""
    
    _instance = None
    _client = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_client(self, websocket_url: str = "ws://localhost:8765") -> StreamlitWebSocketClient:
        """Get or create WebSocket client."""
        if self._client is None:
            self._client = StreamlitWebSocketClient(websocket_url)
            self._client.start()
        return self._client
    
    def cleanup(self):
        """Clean up WebSocket client."""
        if self._client:
            self._client.stop()
            self._client = None


# Streamlit integration functions
def init_websocket_connection(websocket_url: str = "ws://localhost:8765") -> StreamlitWebSocketClient:
    """Initialize WebSocket connection for Streamlit app."""
    if 'websocket_client' not in st.session_state:
        manager = StreamlitWebSocketManager()
        st.session_state.websocket_client = manager.get_client(websocket_url)
    
    return st.session_state.websocket_client


def get_realtime_data(data_type: str, default=None):
    """Get real-time data from WebSocket connection."""
    if 'websocket_client' in st.session_state:
        client = st.session_state.websocket_client
        if client.is_connected():
            return client.get_data(data_type) or default
    
    return default


def is_websocket_connected() -> bool:
    """Check if WebSocket is connected."""
    if 'websocket_client' in st.session_state:
        return st.session_state.websocket_client.is_connected()
    return False


def get_connection_status() -> Dict[str, Any]:
    """Get WebSocket connection status information."""
    if 'websocket_client' in st.session_state:
        client = st.session_state.websocket_client
        return {
            'connected': client.is_connected(),
            'last_updates': client.data_store['last_update']
        }
    
    return {'connected': False, 'last_updates': {}}


# Streamlit component for connection status
def show_websocket_status():
    """Show WebSocket connection status in Streamlit."""
    status = get_connection_status()
    
    if status['connected']:
        st.success("🟢 Real-time data connected")
        
        # Show last update times
        with st.expander("Last Update Times"):
            for data_type, timestamp in status['last_updates'].items():
                if timestamp:
                    st.text(f"{data_type}: {timestamp}")
    else:
        st.error("🔴 Real-time data disconnected - using cached data")
        st.info("The dashboard will fall back to periodic refresh mode.")


# Cleanup function for Streamlit
def cleanup_websocket():
    """Clean up WebSocket connection when Streamlit app closes."""
    if 'websocket_client' in st.session_state:
        st.session_state.websocket_client.stop()
        del st.session_state.websocket_client
