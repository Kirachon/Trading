"""
Market Data Ingestion Service
Streams real-time and historical market data from exchanges.
"""
import asyncio
import logging
import os
import sys
import json
import time
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any
import pandas as pd
import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

# Add shared modules to path
sys.path.append('/app')
from shared.exchange_factory import get_exchange
from shared.messaging import RedisPublisher
from shared.rate_limiter import ExchangeRateLimiters
from shared.exceptions import (
    MarketDataError, MarketDataUnavailableError, DatabaseError,
    RecoveryStrategy, log_exception_context
)
from shared.metrics import get_metrics_collector, metrics_timer
from shared.tracing import get_tracer
from shared.health import HealthChecker, check_redis_connection, check_mongodb_connection

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

        # Initialize metrics and tracing
        self.metrics = get_metrics_collector('market_data', port=8000)
        self.tracer = get_tracer('market_data')

        # Initialize health checker
        self.health_checker = HealthChecker('market_data', port=8080)
        self._setup_health_checks()

        # Data caching and compression
        self.data_cache = {}  # In-memory cache for frequently accessed data
        self.cache_ttl = 300  # 5 minutes cache TTL
        self.compression_enabled = True

        # Batch processing configuration
        self.batch_size = 5000  # Larger batch size for efficiency
        self.max_concurrent_fetches = 5  # Limit concurrent API calls

        # Configuration
        self.exchanges = ['binance']  # Start with Binance
        self.symbols = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'ADA/USDT', 'SOL/USDT']
        self.timeframes = ['1m', '5m', '15m', '1h', '4h', '1d']

        self.running = False

    def _setup_health_checks(self):
        """Setup health check dependencies and custom checks."""
        # Register dependency checks
        async def check_redis():
            return await check_redis_connection(self.redis_url)

        async def check_mongodb():
            return await check_mongodb_connection(self.mongodb_url)

        self.health_checker.register_dependency_check('redis', check_redis)
        self.health_checker.register_dependency_check('mongodb', check_mongodb)

        # Register custom health checks
        self.health_checker.register_custom_check(
            'data_freshness', self._check_data_freshness
        )
        self.health_checker.register_custom_check(
            'exchange_connectivity', self._check_exchange_connectivity
        )

    async def _check_data_freshness(self) -> Dict[str, Any]:
        """Check if market data is fresh."""
        try:
            # Check if we have recent data in cache
            fresh_data_count = 0
            stale_data_count = 0

            for key, cache_entry in self.data_cache.items():
                age_seconds = (datetime.now() - cache_entry['timestamp']).total_seconds()
                if age_seconds < self.cache_ttl:
                    fresh_data_count += 1
                else:
                    stale_data_count += 1

            status = 'healthy' if fresh_data_count > 0 or not self.data_cache else 'warning'

            return {
                'status': status,
                'fresh_data_count': fresh_data_count,
                'stale_data_count': stale_data_count,
                'cache_size': len(self.data_cache)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    async def _check_exchange_connectivity(self) -> Dict[str, Any]:
        """Check connectivity to configured exchanges."""
        try:
            connectivity_status = {}
            overall_status = 'healthy'

            for exchange_id in self.exchanges:
                try:
                    exchange = await get_exchange(exchange_id, use_testnet=True)
                    # Simple connectivity test
                    await exchange.load_markets()
                    connectivity_status[exchange_id] = 'connected'
                except Exception as e:
                    connectivity_status[exchange_id] = f'error: {str(e)}'
                    overall_status = 'unhealthy'

            return {
                'status': overall_status,
                'exchanges': connectivity_status
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    @metrics_timer('market_data', 'initialize')
    async def initialize(self):
        """Initialize connections and services."""
        try:
            # Start metrics server
            self.metrics.start_metrics_server()

            # Connect to Redis
            await self.redis_publisher.connect()
            self.metrics.set_active_connections('redis', 1)

            # Connect to MongoDB using Motor (async)
            self.mongo_client = AsyncIOMotorClient(self.mongodb_url)
            self.db = self.mongo_client['trading_data']

            # Test the connection
            await self.mongo_client.admin.command('ping')
            self.metrics.set_active_connections('mongodb', 1)

            # Start health check server
            try:
                await self.health_checker.start_server()
                logger.info("Health check server started on port 8080")
            except Exception as e:
                logger.warning(f"Failed to start health check server: {e}")

            logger.info("Market Data Service initialized successfully")

        except Exception as e:
            self.metrics.record_error('initialization_error')
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

            # Create index for timestamp (async)
            await collection.create_index("timestamp", unique=True)
            
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
                            await collection.insert_many(documents, ordered=False)
                            total_fetched += len(documents)
                        except Exception as e:
                            # Handle duplicate key errors and other bulk write errors
                            if "duplicate key" in str(e).lower():
                                # Count successful inserts by attempting individual inserts
                                successful_inserts = 0
                                for doc in documents:
                                    try:
                                        await collection.insert_one(doc)
                                        successful_inserts += 1
                                    except Exception:
                                        pass  # Skip duplicates
                                total_fetched += successful_inserts
                            else:
                                logger.error(f"Error inserting documents: {e}")
                                continue
                    
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

    async def fetch_historical_data_batch(self, requests: list) -> Dict[str, Any]:
        """Fetch historical data for multiple symbols/timeframes in optimized batches."""
        try:
            results = {}
            semaphore = asyncio.Semaphore(self.max_concurrent_fetches)

            async def fetch_single(request):
                async with semaphore:
                    try:
                        exchange_id, symbol, timeframe, days_back = request
                        await self.fetch_historical_data_optimized(exchange_id, symbol, timeframe, days_back)
                        return f"{exchange_id}_{symbol}_{timeframe}", "success"
                    except Exception as e:
                        log_exception_context(e, {'request': request})
                        return f"{exchange_id}_{symbol}_{timeframe}", f"error: {str(e)}"

            # Execute all requests concurrently with limit
            tasks = [fetch_single(req) for req in requests]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, tuple):
                    key, status = result
                    results[key] = status
                else:
                    logger.error(f"Unexpected batch result: {result}")

            return results

        except Exception as e:
            log_exception_context(e, {'batch_size': len(requests)})
            raise MarketDataError(f"Batch historical data fetch failed: {str(e)}")

    async def fetch_historical_data_optimized(self, exchange_id: str, symbol: str,
                                            timeframe: str, days_back: int = 30):
        """Optimized historical data fetching with caching and compression."""
        try:
            # Check cache first
            cache_key = f"{exchange_id}_{symbol}_{timeframe}_hist"
            if cache_key in self.data_cache:
                cache_entry = self.data_cache[cache_key]
                if (datetime.now() - cache_entry['timestamp']).seconds < self.cache_ttl:
                    logger.debug(f"Using cached data for {symbol} {timeframe}")
                    return cache_entry['data']

            exchange = await get_exchange(exchange_id, use_testnet=True)
            rate_limiter = self.rate_limiters.get_limiter(exchange_id)

            # Calculate start time
            start_date = datetime.now() - timedelta(days=days_back)
            since = int(start_date.timestamp() * 1000)

            # Collection name
            collection_name = f"{exchange_id}_{symbol.replace('/', '_')}_{timeframe}"
            collection = self.db[collection_name]

            # Create optimized indexes
            await self._create_optimized_indexes(collection)

            # Check existing data to avoid re-fetching
            existing_range = await self._get_existing_data_range(collection)
            if existing_range and existing_range['latest'] >= since:
                logger.info(f"Data already exists for {symbol} {timeframe}, skipping fetch")
                return

            # Use larger batch size for efficiency
            limit = self.batch_size
            total_fetched = 0

            logger.info(f"Optimized fetch for {symbol} {timeframe} from {exchange_id}")

            while True:
                await rate_limiter.acquire()

                try:
                    ohlcvs = await exchange.fetch_ohlcv(symbol, timeframe, since, limit)

                    if not ohlcvs:
                        break

                    # Process batch efficiently
                    batch_result = await self._process_ohlcv_batch_optimized(
                        ohlcvs, collection, symbol, timeframe, exchange_id
                    )

                    total_fetched += batch_result['inserted']

                    # Update since for next batch
                    since = ohlcvs[-1][0] + 1

                    # Break if we got less than requested
                    if len(ohlcvs) < limit:
                        break

                    await asyncio.sleep(0.05)  # Smaller delay for efficiency

                except Exception as e:
                    if "rate limit" in str(e).lower():
                        await asyncio.sleep(10)
                        continue
                    else:
                        log_exception_context(e, {'symbol': symbol, 'timeframe': timeframe})
                        break

            logger.info(f"Optimized fetch completed: {total_fetched} records for {symbol} {timeframe}")

            # Update cache
            self.data_cache[cache_key] = {
                'data': total_fetched,
                'timestamp': datetime.now()
            }

        except Exception as e:
            log_exception_context(e, {'symbol': symbol, 'timeframe': timeframe})
            raise MarketDataError(f"Optimized historical data fetch failed: {str(e)}")

    async def _create_optimized_indexes(self, collection):
        """Create optimized database indexes."""
        try:
            await collection.create_index([
                ("timestamp", 1),
                ("symbol", 1),
                ("timeframe", 1)
            ], unique=True, background=True)

            await collection.create_index("datetime", background=True)
            await collection.create_index("symbol", background=True)

        except Exception as e:
            logger.warning(f"Error creating indexes: {e}")

    async def _get_existing_data_range(self, collection):
        """Get existing data range to optimize fetching."""
        try:
            pipeline = [
                {"$group": {
                    "_id": None,
                    "earliest": {"$min": "$timestamp"},
                    "latest": {"$max": "$timestamp"},
                    "count": {"$sum": 1}
                }}
            ]

            result = await collection.aggregate(pipeline).to_list(length=1)
            return result[0] if result else None

        except Exception as e:
            logger.warning(f"Error checking data range: {e}")
            return None

    async def _process_ohlcv_batch_optimized(self, ohlcvs: list, collection,
                                           symbol: str, timeframe: str, exchange_id: str):
        """Process OHLCV batch with optimized bulk operations."""
        try:
            if not ohlcvs:
                return {'inserted': 0, 'duplicates': 0}

            # Prepare bulk operations
            operations = []
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

                operations.append({
                    'replaceOne': {
                        'filter': {'timestamp': doc['timestamp']},
                        'replacement': doc,
                        'upsert': True
                    }
                })

            # Execute bulk operation
            result = await collection.bulk_write(operations, ordered=False)

            return {
                'inserted': result.upserted_count + result.modified_count,
                'duplicates': len(ohlcvs) - (result.upserted_count + result.modified_count)
            }

        except Exception as e:
            log_exception_context(e, {'symbol': symbol, 'batch_size': len(ohlcvs)})
            raise DatabaseError(f"Optimized batch processing failed: {str(e)}")

    @metrics_timer('market_data', 'stream_real_time_data')
    async def stream_real_time_data(self, exchange_id: str):
        """Stream real-time market data."""
        with self.tracer.start_span('stream_real_time_data', {'exchange': exchange_id}):
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
                                    start_time = time.time()
                                    ohlcv = await exchange.watch_ohlcv(symbol, timeframe)

                                    if ohlcv:
                                        # Record market data latency
                                        latency = time.time() - start_time
                                        self.metrics.record_market_data_latency(exchange_id, symbol, latency)

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
                                    self.metrics.record_error('streaming_error')
                                    logger.error(f"Error streaming {symbol} {timeframe}: {e}")
                                    await asyncio.sleep(1)

                    except Exception as e:
                        self.metrics.record_error('streaming_cycle_error')
                        logger.error(f"Error in real-time streaming: {e}")
                        await asyncio.sleep(5)

            except Exception as e:
                self.metrics.record_error('streaming_initialization_error')
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
            
            # Insert or update (upsert) - async
            await collection.replace_one(
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
            
            # Prepare batch requests for optimized historical data fetching
            batch_requests = []
            for exchange_id in self.exchanges:
                for symbol in self.symbols:
                    for timeframe in self.timeframes:
                        batch_requests.append((exchange_id, symbol, timeframe, 30))

            # Execute batch historical data fetching
            if batch_requests:
                logger.info(f"Starting optimized batch fetch for {len(batch_requests)} requests")
                batch_results = await self.fetch_historical_data_batch(batch_requests)

                # Log results
                successful = sum(1 for status in batch_results.values() if status == "success")
                failed = len(batch_results) - successful
                logger.info(f"Historical data fetching completed: {successful} successful, {failed} failed")

                if failed > 0:
                    logger.warning("Some historical data requests failed:")
                    for key, status in batch_results.items():
                        if status != "success":
                            logger.warning(f"  {key}: {status}")
            
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
