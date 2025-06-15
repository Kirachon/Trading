#!/usr/bin/env python3
"""
WebSocket Server Startup Script

This script starts the WebSocket server for real-time data streaming
to the Streamlit dashboard.
"""

import asyncio
import logging
import os
import sys
import signal
from websocket_server import start_websocket_server, websocket_streamer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class WebSocketServerManager:
    """Manager for the WebSocket server lifecycle."""
    
    def __init__(self):
        self.running = False
        self.server_task = None
        
    async def start(self):
        """Start the WebSocket server."""
        try:
            self.running = True
            
            # Get configuration from environment
            host = os.getenv('WEBSOCKET_HOST', '0.0.0.0')
            port = int(os.getenv('WEBSOCKET_PORT', '8765'))
            
            logger.info(f"Starting WebSocket server on {host}:{port}")
            
            # Start the server
            self.server_task = asyncio.create_task(
                start_websocket_server(host, port)
            )
            
            await self.server_task
            
        except Exception as e:
            logger.error(f"Error starting WebSocket server: {e}")
            raise
    
    async def stop(self):
        """Stop the WebSocket server."""
        try:
            self.running = False
            
            if self.server_task:
                self.server_task.cancel()
                try:
                    await self.server_task
                except asyncio.CancelledError:
                    pass
            
            # Cleanup resources
            await websocket_streamer.cleanup()
            
            logger.info("WebSocket server stopped")
            
        except Exception as e:
            logger.error(f"Error stopping WebSocket server: {e}")


# Global server manager
server_manager = WebSocketServerManager()


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    
    # Create new event loop for cleanup if needed
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Schedule shutdown
    loop.create_task(server_manager.stop())


async def main():
    """Main function."""
    try:
        # Set up signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start the server
        await server_manager.start()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        await server_manager.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shutdown complete")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
