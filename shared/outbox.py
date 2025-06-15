"""
Transactional Outbox Pattern Implementation

This module provides the infrastructure for reliable message delivery using the
transactional outbox pattern. Messages are stored in the database within the same
transaction as business logic, then published by a separate process.
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional
import asyncpg
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class OutboxStatus(Enum):
    """Status of outbox messages."""
    PENDING = "pending"
    PUBLISHED = "published"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class OutboxMessage:
    """Represents a message in the outbox."""
    id: str
    aggregate_id: str
    aggregate_type: str
    event_type: str
    payload: Dict[str, Any]
    status: OutboxStatus
    created_at: datetime
    published_at: Optional[datetime] = None
    retry_count: int = 0
    error_message: Optional[str] = None


class OutboxRepository:
    """Repository for managing outbox messages."""
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
    
    async def create_table(self):
        """Create the outbox table if it doesn't exist."""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS outbox (
                    id UUID PRIMARY KEY,
                    aggregate_id VARCHAR(255) NOT NULL,
                    aggregate_type VARCHAR(100) NOT NULL,
                    event_type VARCHAR(100) NOT NULL,
                    payload JSONB NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    published_at TIMESTAMP WITH TIME ZONE,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    error_message TEXT,
                    INDEX (status, created_at),
                    INDEX (aggregate_id),
                    INDEX (aggregate_type)
                )
            """)
    
    async def add_message(
        self, 
        conn: asyncpg.Connection,
        aggregate_id: str,
        aggregate_type: str,
        event_type: str,
        payload: Dict[str, Any]
    ) -> str:
        """Add a message to the outbox within a transaction."""
        message_id = str(uuid.uuid4())
        
        await conn.execute("""
            INSERT INTO outbox (id, aggregate_id, aggregate_type, event_type, payload)
            VALUES ($1, $2, $3, $4, $5)
        """, message_id, aggregate_id, aggregate_type, event_type, json.dumps(payload))
        
        logger.debug(f"Added outbox message: {message_id}")
        return message_id
    
    async def get_pending_messages(self, limit: int = 100) -> List[OutboxMessage]:
        """Get pending messages for publishing."""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, aggregate_id, aggregate_type, event_type, payload,
                       status, created_at, published_at, retry_count, error_message
                FROM outbox
                WHERE status IN ('pending', 'retrying')
                ORDER BY created_at ASC
                LIMIT $1
            """, limit)
            
            return [
                OutboxMessage(
                    id=row['id'],
                    aggregate_id=row['aggregate_id'],
                    aggregate_type=row['aggregate_type'],
                    event_type=row['event_type'],
                    payload=json.loads(row['payload']),
                    status=OutboxStatus(row['status']),
                    created_at=row['created_at'],
                    published_at=row['published_at'],
                    retry_count=row['retry_count'],
                    error_message=row['error_message']
                )
                for row in rows
            ]
    
    async def mark_published(self, message_id: str):
        """Mark a message as successfully published."""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE outbox
                SET status = 'published', published_at = NOW()
                WHERE id = $1
            """, message_id)
    
    async def mark_failed(self, message_id: str, error_message: str):
        """Mark a message as failed."""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE outbox
                SET status = 'failed', error_message = $2, retry_count = retry_count + 1
                WHERE id = $1
            """, message_id, error_message)
    
    async def mark_retrying(self, message_id: str):
        """Mark a message for retry."""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE outbox
                SET status = 'retrying', retry_count = retry_count + 1
                WHERE id = $1
            """, message_id)
    
    async def cleanup_old_messages(self, days: int = 7):
        """Clean up old published messages."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        async with self.db_pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM outbox
                WHERE status = 'published' AND published_at < $1
            """, cutoff_date)
            
            logger.info(f"Cleaned up {result} old outbox messages")


class OutboxPublisher:
    """Publisher for processing outbox messages."""
    
    def __init__(self, repository: OutboxRepository, message_publisher):
        self.repository = repository
        self.message_publisher = message_publisher
        self.running = False
        self.max_retries = 3
        self.retry_delay = 5  # seconds
    
    async def start(self):
        """Start the outbox publisher."""
        self.running = True
        logger.info("Outbox publisher started")
        
        while self.running:
            try:
                await self.process_messages()
                await asyncio.sleep(1)  # Check for new messages every second
            except Exception as e:
                logger.error(f"Error in outbox publisher: {e}")
                await asyncio.sleep(5)  # Wait before retrying
    
    async def stop(self):
        """Stop the outbox publisher."""
        self.running = False
        logger.info("Outbox publisher stopped")
    
    async def process_messages(self):
        """Process pending outbox messages."""
        messages = await self.repository.get_pending_messages()
        
        for message in messages:
            try:
                await self.publish_message(message)
                await self.repository.mark_published(message.id)
                logger.debug(f"Published outbox message: {message.id}")
                
            except Exception as e:
                logger.error(f"Failed to publish message {message.id}: {e}")
                
                if message.retry_count >= self.max_retries:
                    await self.repository.mark_failed(message.id, str(e))
                    logger.error(f"Message {message.id} failed after {self.max_retries} retries")
                else:
                    await self.repository.mark_retrying(message.id)
                    logger.warning(f"Message {message.id} will be retried")
    
    async def publish_message(self, message: OutboxMessage):
        """Publish a single message."""
        # Create the message payload for RabbitMQ
        routing_key = f"{message.aggregate_type}.{message.event_type}"
        
        await self.message_publisher.publish(
            routing_key=routing_key,
            message={
                "id": message.id,
                "aggregate_id": message.aggregate_id,
                "aggregate_type": message.aggregate_type,
                "event_type": message.event_type,
                "payload": message.payload,
                "timestamp": message.created_at.isoformat()
            }
        )


# Helper function for services to use the outbox pattern
async def add_outbox_message(
    conn: asyncpg.Connection,
    repository: OutboxRepository,
    aggregate_id: str,
    aggregate_type: str,
    event_type: str,
    payload: Dict[str, Any]
) -> str:
    """Helper function to add a message to the outbox."""
    return await repository.add_message(
        conn, aggregate_id, aggregate_type, event_type, payload
    )
