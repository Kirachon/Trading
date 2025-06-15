"""
Outbox Publisher Service

This service is responsible for publishing messages from the outbox table
to RabbitMQ, ensuring reliable message delivery across the trading system.
"""

import asyncio
import logging
import os
import signal
import sys
from typing import Optional

import asyncpg
from shared.outbox import OutboxRepository, OutboxPublisher
from shared.messaging import RabbitMQPublisher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OutboxPublisherService:
    """Main outbox publisher service."""
    
    def __init__(self):
        self.db_pool: Optional[asyncpg.Pool] = None
        self.repository: Optional[OutboxRepository] = None
        self.publisher: Optional[OutboxPublisher] = None
        self.rabbitmq_publisher: Optional[RabbitMQPublisher] = None
        self.running = False
        
        # Configuration
        self.postgres_url = os.getenv(
            'POSTGRES_URL',
            'postgresql://trading_user:SecureDB2024!@localhost:5432/trading_db'
        )
        self.rabbitmq_url = os.getenv(
            'RABBITMQ_URL',
            'amqp://guest:guest@localhost:5672/'
        )
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(self.shutdown())
    
    async def initialize(self):
        """Initialize the service."""
        try:
            # Create database connection pool
            self.db_pool = await asyncpg.create_pool(self.postgres_url)
            logger.info("Connected to PostgreSQL")
            
            # Initialize outbox repository
            self.repository = OutboxRepository(self.db_pool)
            await self.repository.create_table()
            logger.info("Outbox table initialized")
            
            # Initialize RabbitMQ publisher
            self.rabbitmq_publisher = RabbitMQPublisher(self.rabbitmq_url)
            await self.rabbitmq_publisher.connect()
            logger.info("Connected to RabbitMQ")
            
            # Initialize outbox publisher
            self.publisher = OutboxPublisher(self.repository, self.rabbitmq_publisher)
            
            logger.info("Outbox Publisher Service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize service: {e}")
            raise
    
    async def start(self):
        """Start the service."""
        self.running = True
        logger.info("Starting Outbox Publisher Service...")
        
        try:
            # Start the outbox publisher
            await self.publisher.start()
            
        except Exception as e:
            logger.error(f"Error in service: {e}")
            raise
    
    async def shutdown(self):
        """Shutdown the service gracefully."""
        logger.info("Shutting down Outbox Publisher Service...")
        self.running = False
        
        try:
            # Stop the publisher
            if self.publisher:
                await self.publisher.stop()
            
            # Close RabbitMQ connection
            if self.rabbitmq_publisher:
                await self.rabbitmq_publisher.close()
            
            # Close database pool
            if self.db_pool:
                await self.db_pool.close()
            
            logger.info("Outbox Publisher Service shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    async def health_check(self) -> bool:
        """Check service health."""
        try:
            if not self.db_pool:
                return False
            
            # Test database connection
            async with self.db_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            
            # Test RabbitMQ connection
            if not self.rabbitmq_publisher or not self.rabbitmq_publisher.connection:
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    async def cleanup_old_messages(self):
        """Periodic cleanup of old messages."""
        if self.repository:
            await self.repository.cleanup_old_messages()


async def main():
    """Main entry point."""
    service = OutboxPublisherService()
    
    try:
        await service.initialize()
        
        # Start cleanup task
        cleanup_task = asyncio.create_task(periodic_cleanup(service))
        
        # Start the main service
        await service.start()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Service failed: {e}")
        sys.exit(1)
    finally:
        await service.shutdown()


async def periodic_cleanup(service: OutboxPublisherService):
    """Periodic cleanup task."""
    while service.running:
        try:
            await asyncio.sleep(3600)  # Run every hour
            await service.cleanup_old_messages()
        except Exception as e:
            logger.error(f"Error in cleanup task: {e}")


if __name__ == "__main__":
    asyncio.run(main())
