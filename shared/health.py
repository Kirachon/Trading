"""
Comprehensive Health Check System for Trading Services

This module provides health check endpoints and monitoring capabilities
for all trading system services.
"""

import os
import time
import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import aiohttp
from aiohttp import web
import json

logger = logging.getLogger(__name__)


class HealthChecker:
    """Comprehensive health checker for trading services."""
    
    def __init__(self, service_name: str, port: int = 8080):
        self.service_name = service_name
        self.port = port
        self.app = web.Application()
        self.setup_routes()
        
        # Health check state
        self.startup_time = datetime.now()
        self.last_health_check = None
        self.dependency_status = {}
        self.custom_checks = {}
        
    def setup_routes(self):
        """Setup health check routes."""
        self.app.router.add_get('/health', self.health_endpoint)
        self.app.router.add_get('/health/ready', self.readiness_endpoint)
        self.app.router.add_get('/health/live', self.liveness_endpoint)
        self.app.router.add_get('/health/detailed', self.detailed_health_endpoint)
        
    async def health_endpoint(self, request):
        """Basic health check endpoint."""
        try:
            health_status = await self.check_basic_health()
            status_code = 200 if health_status['status'] == 'healthy' else 503
            return web.json_response(health_status, status=status_code)
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return web.json_response({
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': time.time()
            }, status=503)
    
    async def readiness_endpoint(self, request):
        """Readiness probe - checks if service is ready to accept traffic."""
        try:
            readiness_status = await self.check_readiness()
            status_code = 200 if readiness_status['ready'] else 503
            return web.json_response(readiness_status, status=status_code)
        except Exception as e:
            logger.error(f"Readiness check failed: {e}")
            return web.json_response({
                'ready': False,
                'error': str(e),
                'timestamp': time.time()
            }, status=503)
    
    async def liveness_endpoint(self, request):
        """Liveness probe - checks if service is alive."""
        return web.json_response({
            'alive': True,
            'service': self.service_name,
            'uptime_seconds': (datetime.now() - self.startup_time).total_seconds(),
            'timestamp': time.time()
        })
    
    async def detailed_health_endpoint(self, request):
        """Detailed health information including dependencies."""
        try:
            detailed_status = await self.check_detailed_health()
            status_code = 200 if detailed_status['status'] == 'healthy' else 503
            return web.json_response(detailed_status, status=status_code)
        except Exception as e:
            logger.error(f"Detailed health check failed: {e}")
            return web.json_response({
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': time.time()
            }, status=503)
    
    async def check_basic_health(self) -> Dict[str, Any]:
        """Perform basic health checks."""
        health_status = {
            'status': 'healthy',
            'service': self.service_name,
            'timestamp': time.time(),
            'uptime_seconds': (datetime.now() - self.startup_time).total_seconds()
        }
        
        # Check memory usage if psutil is available
        try:
            import psutil
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 90:
                health_status['status'] = 'unhealthy'
                health_status['memory_warning'] = f'High memory usage: {memory_percent:.1f}%'
        except ImportError:
            pass
        
        self.last_health_check = datetime.now()
        return health_status
    
    async def check_readiness(self) -> Dict[str, Any]:
        """Check if service is ready to accept traffic."""
        readiness_status = {
            'ready': True,
            'service': self.service_name,
            'timestamp': time.time(),
            'dependencies': {}
        }
        
        # Check all registered dependencies
        for dep_name, dep_check in self.dependency_status.items():
            try:
                is_ready = await dep_check()
                readiness_status['dependencies'][dep_name] = 'ready' if is_ready else 'not_ready'
                if not is_ready:
                    readiness_status['ready'] = False
            except Exception as e:
                readiness_status['dependencies'][dep_name] = f'error: {str(e)}'
                readiness_status['ready'] = False
        
        return readiness_status
    
    async def check_detailed_health(self) -> Dict[str, Any]:
        """Perform comprehensive health checks."""
        detailed_status = {
            'status': 'healthy',
            'service': self.service_name,
            'version': os.getenv('SERVICE_VERSION', '1.0.0'),
            'timestamp': time.time(),
            'uptime_seconds': (datetime.now() - self.startup_time).total_seconds(),
            'startup_time': self.startup_time.isoformat(),
            'last_health_check': self.last_health_check.isoformat() if self.last_health_check else None,
            'dependencies': {},
            'custom_checks': {},
            'environment': {
                'python_version': os.sys.version,
                'service_name': self.service_name,
                'container_id': os.getenv('HOSTNAME', 'unknown')
            }
        }
        
        # Check dependencies
        for dep_name, dep_check in self.dependency_status.items():
            try:
                is_healthy = await dep_check()
                detailed_status['dependencies'][dep_name] = 'healthy' if is_healthy else 'unhealthy'
                if not is_healthy:
                    detailed_status['status'] = 'degraded'
            except Exception as e:
                detailed_status['dependencies'][dep_name] = f'error: {str(e)}'
                detailed_status['status'] = 'unhealthy'
        
        # Run custom health checks
        for check_name, check_func in self.custom_checks.items():
            try:
                check_result = await check_func()
                detailed_status['custom_checks'][check_name] = check_result
                if isinstance(check_result, dict) and check_result.get('status') == 'unhealthy':
                    detailed_status['status'] = 'unhealthy'
            except Exception as e:
                detailed_status['custom_checks'][check_name] = {'status': 'error', 'message': str(e)}
                detailed_status['status'] = 'unhealthy'
        
        self.last_health_check = datetime.now()
        return detailed_status
    
    def register_dependency_check(self, name: str, check_func):
        """Register a dependency health check function."""
        self.dependency_status[name] = check_func
    
    def register_custom_check(self, name: str, check_func):
        """Register a custom health check function."""
        self.custom_checks[name] = check_func
    
    async def start_server(self):
        """Start the health check server."""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        logger.info(f"Health check server started on port {self.port}")
        return runner


# Utility functions for common dependency checks
async def check_redis_connection(redis_url: str) -> bool:
    """Check Redis connection."""
    try:
        import aioredis
        redis = aioredis.from_url(redis_url)
        await redis.ping()
        await redis.close()
        return True
    except Exception:
        return False


async def check_mongodb_connection(mongodb_url: str) -> bool:
    """Check MongoDB connection."""
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
        client = AsyncIOMotorClient(mongodb_url)
        await client.admin.command('ping')
        client.close()
        return True
    except Exception:
        return False


async def check_postgresql_connection(postgres_url: str) -> bool:
    """Check PostgreSQL connection."""
    try:
        import asyncpg
        conn = await asyncpg.connect(postgres_url)
        await conn.execute('SELECT 1')
        await conn.close()
        return True
    except Exception:
        return False


async def check_rabbitmq_connection(rabbitmq_url: str) -> bool:
    """Check RabbitMQ connection."""
    try:
        import aio_pika
        connection = await aio_pika.connect_robust(rabbitmq_url)
        await connection.close()
        return True
    except Exception:
        return False
