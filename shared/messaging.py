"""
Messaging utilities for RabbitMQ and Redis Pub/Sub.
"""
import aio_pika
import redis.asyncio as redis
import json
import logging
from typing import Dict, Any, Callable, Optional
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class MessagePublisher(ABC):
    """Abstract base class for message publishers."""
    
    @abstractmethod
    async def publish(self, message: Dict[str, Any], routing_key: str = None):
        """Publish a message."""
        pass
    
    @abstractmethod
    async def close(self):
        """Close the publisher."""
        pass

class RabbitMQPublisher(MessagePublisher):
    """RabbitMQ message publisher for reliable delivery."""
    
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.connection = None
        self.channel = None
        self.exchange = None
    
    async def connect(self):
        """Establish connection to RabbitMQ."""
        try:
            self.connection = await aio_pika.connect_robust(self.connection_url)
            self.channel = await self.connection.channel()
            self.exchange = await self.channel.declare_exchange(
                "trading", 
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            logger.info("Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    async def publish(self, message: Dict[str, Any], routing_key: str):
        """Publish message to RabbitMQ."""
        if not self.exchange:
            await self.connect()
        
        try:
            await self.exchange.publish(
                aio_pika.Message(
                    body=json.dumps(message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=routing_key
            )
            logger.debug(f"Published message to {routing_key}")
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            raise
    
    async def close(self):
        """Close RabbitMQ connection."""
        try:
            if self.connection:
                await self.connection.close()
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}")

class RedisPublisher(MessagePublisher):
    """Redis Pub/Sub publisher for high-frequency data."""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis_client = None
    
    async def connect(self):
        """Establish connection to Redis."""
        try:
            self.redis_client = redis.from_url(self.redis_url)
            await self.redis_client.ping()
            logger.info("Connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def publish(self, message: Dict[str, Any], routing_key: str):
        """Publish message to Redis channel."""
        if not self.redis_client:
            await self.connect()
        
        try:
            await self.redis_client.publish(routing_key, json.dumps(message))
            logger.debug(f"Published message to Redis channel {routing_key}")
        except Exception as e:
            logger.error(f"Failed to publish to Redis: {e}")
            raise
    
    async def close(self):
        """Close Redis connection."""
        try:
            if self.redis_client:
                await self.redis_client.close()
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

class MessageConsumer:
    """Message consumer for RabbitMQ queues."""
    
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.connection = None
        self.channel = None
    
    async def connect(self):
        """Establish connection to RabbitMQ."""
        try:
            self.connection = await aio_pika.connect_robust(self.connection_url)
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)
            logger.info("Consumer connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect consumer to RabbitMQ: {e}")
            raise
    
    async def consume(self, queue_name: str, callback: Callable, routing_key: str = None):
        """Start consuming messages from a queue."""
        if not self.channel:
            await self.connect()
        
        try:
            # Declare exchange and queue
            exchange = await self.channel.declare_exchange(
                "trading", 
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            
            queue = await self.channel.declare_queue(
                queue_name, 
                durable=True
            )
            
            if routing_key:
                await queue.bind(exchange, routing_key)
            
            # Start consuming
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        try:
                            data = json.loads(message.body.decode())
                            await callback(data)
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            
        except Exception as e:
            logger.error(f"Error in message consumer: {e}")
            raise
    
    async def close(self):
        """Close consumer connection."""
        try:
            if self.connection:
                await self.connection.close()
        except Exception as e:
            logger.error(f"Error closing consumer connection: {e}")

class RedisSubscriber:
    """Redis Pub/Sub subscriber for high-frequency data."""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis_client = None
        self.pubsub = None
    
    async def connect(self):
        """Establish connection to Redis."""
        try:
            self.redis_client = redis.from_url(self.redis_url)
            self.pubsub = self.redis_client.pubsub()
            await self.redis_client.ping()
            logger.info("Subscriber connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect subscriber to Redis: {e}")
            raise
    
    async def subscribe(self, channels: list, callback: Callable):
        """Subscribe to Redis channels."""
        if not self.pubsub:
            await self.connect()
        
        try:
            await self.pubsub.subscribe(*channels)
            
            async for message in self.pubsub.listen():
                if message['type'] == 'message':
                    try:
                        data = json.loads(message['data'])
                        await callback(data, message['channel'].decode())
                    except Exception as e:
                        logger.error(f"Error processing Redis message: {e}")
                        
        except Exception as e:
            logger.error(f"Error in Redis subscriber: {e}")
            raise
    
    async def close(self):
        """Close subscriber connection."""
        try:
            if self.pubsub:
                await self.pubsub.close()
            if self.redis_client:
                await self.redis_client.close()
        except Exception as e:
            logger.error(f"Error closing Redis subscriber: {e}")
