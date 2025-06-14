"""
Market Data Ingestion Service
Streams real-time and historical market data from exchanges.
"""
import asyncio
import logging
import os
import sys
import json
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any
import pandas as pd
import pymongo
from pymongo import MongoClient

# Add shared modules to path
sys.path.append('/app')
from shared.exchange_factory import get_exchange
from shared.messaging import RedisPublisher
from shared.rate_limiter import ExchangeRateLimiters

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MarketDataService:
    """Market data ingestion and streaming service."""
    
    def __init__(self):
        self.redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.mongodb_url = os.getenv('MONGODB_URL', 'mongodb://localhost:27017')
        
        self.redis_publisher = RedisPublisher(self.redis_url)
        self.rate_limiters = ExchangeRateLimiters(self.redis_url)
        
        # MongoDB client for historical data storage
        self.mongo_client = None
        self.db = None
        
        # Configuration
        self.exchanges = ['binance']  # Start with Binance
        self.symbols = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'ADA/USDT', 'SOL/USDT']
        self.timeframes = ['1m', '5m', '15m', '1h', '4h', '1d']
        
        self.running = False
    
    async def initialize(self):
        """Initialize connections and services."""
        try:
            # Connect to Redis
            await self.redis_publisher.connect()
            
            # Connect to MongoDB
            self.mongo_client = MongoClient(self.mongodb_url)
            self.db = self.mongo_client['trading_data']
            
            logger.info("Market Data Service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Market Data Service: {e}")
            raise
    
    async def fetch_historical_data(self, exchange_id: str, symbol: str, 
                                  timeframe: str, days_back: int = 30):
        """Fetch and store historical OHLCV data."""
        try:
            exchange = await get_exchange(exchange_id, use_testnet=True)
            rate_limiter = self.rate_limiters.get_limiter(exchange_id)
            
            # Calculate start time
            start_date = datetime.now() - timedelta(days=days_back)
            since = int(start_date.timestamp() * 1000)
            
            # Collection name
            collection_name = f"{exchange_id}_{symbol.replace('/', '_')}_{timeframe}"
            collection = self.db[collection_name]
            
            # Create index for timestamp
            collection.create_index("timestamp", unique=True)
            
            limit = 1000
            total_fetched = 0
            
            logger.info(f"Fetching historical data for {symbol} {timeframe} from {exchange_id}")
            
            while True:
                # Rate limiting
                await rate_limiter.acquire()
                
                try:
                    ohlcvs = await exchange.fetch_ohlcv(symbol, timeframe, since, limit)
                    
                    if not ohlcvs:
                        break
                    
                    # Prepare documents for MongoDB
                    documents = []
                    for ohlcv in ohlcvs:
                        doc = {
                            'timestamp': ohlcv[0],
                            'datetime': datetime.fromtimestamp(ohlcv[0] / 1000),
                            'open': float(ohlcv[1]),
                            'high': float(ohlcv[2]),
                            'low': float(ohlcv[3]),
                            'close': float(ohlcv[4]),
                            'volume': float(ohlcv[5]),
                            'symbol': symbol,
                            'timeframe': timeframe,
                            'exchange': exchange_id
                        }
                        documents.append(doc)
                    
                    # Insert documents (ignore duplicates)
                    if documents:
                        try:
                            collection.insert_many(documents, ordered=False)
                            total_fetched += len(documents)
                        except pymongo.errors.BulkWriteError as e:
                            # Ignore duplicate key errors
                            inserted = len(documents) - len(e.details['writeErrors'])
                            total_fetched += inserted
                    
                    # Update since for next batch
                    since = ohlcvs[-1][0] + 1
                    
                    # Small delay to avoid overwhelming the exchange
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error fetching historical data: {e}")
                    await asyncio.sleep(5)
                    continue
            
            logger.info(f"Fetched {total_fetched} historical records for {symbol} {timeframe}")
            
        except Exception as e:
            logger.error(f"Failed to fetch historical data: {e}")
    
    async def stream_real_time_data(self, exchange_id: str):
        """Stream real-time market data."""
        try:
            exchange = await get_exchange(exchange_id, use_testnet=True)
            
            # Check if exchange supports WebSocket
            if not exchange.has['watchOHLCV']:
                logger.warning(f"{exchange_id} doesn't support WebSocket OHLCV, using REST polling")
                await self.poll_market_data(exchange_id)
                return
            
            logger.info(f"Starting real-time data stream for {exchange_id}")
            
            # Subscribe to multiple symbols and timeframes
            while self.running:
                try:
                    for symbol in self.symbols:
                        for timeframe in ['1m', '5m']:  # Focus on short timeframes for real-time
                            try:
                                ohlcv = await exchange.watch_ohlcv(symbol, timeframe)
                                
                                if ohlcv:
                                    # Prepare message for Redis
                                    message = {
                                        'exchange': exchange_id,
                                        'symbol': symbol,
                                        'timeframe': timeframe,
                                        'timestamp': ohlcv[0],
                                        'datetime': datetime.fromtimestamp(ohlcv[0] / 1000).isoformat(),
                                        'open': float(ohlcv[1]),
                                        'high': float(ohlcv[2]),
                                        'low': float(ohlcv[3]),
                                        'close': float(ohlcv[4]),
                                        'volume': float(ohlcv[5])
                                    }
                                    
                                    # Publish to Redis
                                    channel = f"market-data:{exchange_id}:ohlcv"
                                    await self.redis_publisher.publish(message, channel)
                                    
                                    # Also store in MongoDB for persistence
                                    await self.store_real_time_data(message)
                                    
                            except Exception as e:
                                logger.error(f"Error streaming {symbol} {timeframe}: {e}")
                                await asyncio.sleep(1)
                
                except Exception as e:
                    logger.error(f"Error in real-time streaming: {e}")
                    await asyncio.sleep(5)
                    
        except Exception as e:
            logger.error(f"Failed to start real-time streaming: {e}")
    
    async def poll_market_data(self, exchange_id: str):
        """Poll market data using REST API (fallback for exchanges without WebSocket)."""
        try:
            exchange = await get_exchange(exchange_id, use_testnet=True)
            rate_limiter = self.rate_limiters.get_limiter(exchange_id)
            
            logger.info(f"Starting REST polling for {exchange_id}")
            
            while self.running:
                try:
                    for symbol in self.symbols:
                        # Rate limiting
                        await rate_limiter.acquire()
                        
                        try:
                            # Fetch latest OHLCV data
                            ohlcvs = await exchange.fetch_ohlcv(symbol, '1m', limit=1)
                            
                            if ohlcvs:
                                ohlcv = ohlcvs[0]
                                message = {
                                    'exchange': exchange_id,
                                    'symbol': symbol,
                                    'timeframe': '1m',
                                    'timestamp': ohlcv[0],
                                    'datetime': datetime.fromtimestamp(ohlcv[0] / 1000).isoformat(),
                                    'open': float(ohlcv[1]),
                                    'high': float(ohlcv[2]),
                                    'low': float(ohlcv[3]),
                                    'close': float(ohlcv[4]),
                                    'volume': float(ohlcv[5])
                                }
                                
                                # Publish to Redis
                                channel = f"market-data:{exchange_id}:ohlcv"
                                await self.redis_publisher.publish(message, channel)
                                
                                # Store in MongoDB
                                await self.store_real_time_data(message)
                                
                        except Exception as e:
                            logger.error(f"Error polling {symbol}: {e}")
                    
                    # Wait before next poll cycle
                    await asyncio.sleep(10)  # Poll every 10 seconds
                    
                except Exception as e:
                    logger.error(f"Error in polling cycle: {e}")
                    await asyncio.sleep(30)
                    
        except Exception as e:
            logger.error(f"Failed to start polling: {e}")
    
    async def store_real_time_data(self, message: Dict[str, Any]):
        """Store real-time data in MongoDB."""
        try:
            collection_name = f"{message['exchange']}_{message['symbol'].replace('/', '_')}_{message['timeframe']}"
            collection = self.db[collection_name]
            
            # Prepare document
            doc = {
                'timestamp': message['timestamp'],
                'datetime': datetime.fromisoformat(message['datetime']),
                'open': message['open'],
                'high': message['high'],
                'low': message['low'],
                'close': message['close'],
                'volume': message['volume'],
                'symbol': message['symbol'],
                'timeframe': message['timeframe'],
                'exchange': message['exchange']
            }
            
            # Insert or update (upsert)
            collection.replace_one(
                {'timestamp': message['timestamp']},
                doc,
                upsert=True
            )
            
        except Exception as e:
            logger.error(f"Error storing real-time data: {e}")
    
    async def run(self):
        """Main service loop."""
        self.running = True
        
        try:
            await self.initialize()
            
            # Start tasks
            tasks = []
            
            # Fetch historical data for all symbols and timeframes
            for exchange_id in self.exchanges:
                for symbol in self.symbols:
                    for timeframe in self.timeframes:
                        task = asyncio.create_task(
                            self.fetch_historical_data(exchange_id, symbol, timeframe)
                        )
                        tasks.append(task)
            
            # Wait for historical data fetching to complete
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                logger.info("Historical data fetching completed")
            
            # Start real-time streaming
            streaming_tasks = []
            for exchange_id in self.exchanges:
                task = asyncio.create_task(self.stream_real_time_data(exchange_id))
                streaming_tasks.append(task)
            
            # Run streaming tasks
            await asyncio.gather(*streaming_tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error in main service loop: {e}")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources."""
        self.running = False
        
        try:
            await self.redis_publisher.close()
            await self.rate_limiters.close_all()
            
            if self.mongo_client:
                self.mongo_client.close()
                
            logger.info("Market Data Service cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    """Main entry point."""
    service = MarketDataService()
    
    try:
        await service.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Service error: {e}")
    finally:
        await service.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
