#!/usr/bin/env python3
"""
Generic Health Check Script for Trading System Services

This script provides a basic health check that can be customized
for different services through environment variables.
"""

import os
import sys
import socket
import time
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def check_port(host: str, port: int, timeout: int = 5) -> bool:
    """Check if a port is open and accepting connections."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.error, socket.timeout):
        return False


def check_service_health() -> Dict[str, Any]:
    """Perform comprehensive service health checks."""
    health_status = {
        'status': 'healthy',
        'checks': {},
        'timestamp': time.time(),
        'service': os.getenv('SERVICE_NAME', 'unknown'),
        'version': os.getenv('SERVICE_VERSION', '1.0.0')
    }

    # Check if the service process is responsive
    try:
        # Basic Python import test
        import asyncio
        health_status['checks']['python_runtime'] = 'ok'
    except Exception as e:
        health_status['checks']['python_runtime'] = f'error: {e}'
        health_status['status'] = 'unhealthy'

    # Check required libraries based on service type
    service_name = os.getenv('SERVICE_NAME', '')
    if 'strategy' in service_name.lower():
        try:
            import talib
            health_status['checks']['talib'] = 'ok'
        except ImportError:
            health_status['checks']['talib'] = 'missing - technical analysis disabled'
            # Don't mark as unhealthy since TA-Lib is optional

    # Check memory usage
    try:
        import psutil
        memory_percent = psutil.virtual_memory().percent
        health_status['checks']['memory_usage'] = f'{memory_percent:.1f}%'
        if memory_percent > 90:
            health_status['status'] = 'unhealthy'
    except ImportError:
        health_status['checks']['memory_usage'] = 'monitoring_unavailable'
    
    # Check database connections if configured
    postgres_url = os.getenv('POSTGRES_URL')
    if postgres_url:
        try:
            # Extract host and port from URL
            if 'postgresql://' in postgres_url:
                # Simple parsing for health check
                url_parts = postgres_url.split('@')[1].split('/')[0]
                host, port = url_parts.split(':')
                if check_port(host, int(port)):
                    health_status['checks']['postgresql'] = 'ok'
                else:
                    health_status['checks']['postgresql'] = 'unreachable'
                    health_status['status'] = 'unhealthy'
        except Exception as e:
            health_status['checks']['postgresql'] = f'error: {e}'
            health_status['status'] = 'unhealthy'
    
    # Check MongoDB connection if configured
    mongodb_url = os.getenv('MONGODB_URL')
    if mongodb_url:
        try:
            # Enhanced MongoDB connection check
            if 'mongodb://' in mongodb_url:
                # Parse MongoDB URL more robustly
                if '@' in mongodb_url:
                    url_parts = mongodb_url.split('@')[1].split('/')[0]
                else:
                    url_parts = mongodb_url.replace('mongodb://', '').split('/')[0]

                if ':' in url_parts:
                    host, port = url_parts.split(':')
                    port = int(port)
                else:
                    host = url_parts
                    port = 27017

                if check_port(host, port):
                    health_status['checks']['mongodb'] = 'ok'
                    # Additional check for authentication if credentials are present
                    if '@' in mongodb_url and 'admin:' in mongodb_url:
                        health_status['checks']['mongodb_auth'] = 'configured'
                else:
                    health_status['checks']['mongodb'] = 'unreachable'
                    health_status['status'] = 'unhealthy'
        except Exception as e:
            health_status['checks']['mongodb'] = f'error: {e}'
            health_status['status'] = 'unhealthy'
    
    # Check Redis connection if configured
    redis_url = os.getenv('REDIS_URL')
    if redis_url:
        try:
            # Extract host and port from URL
            if 'redis://' in redis_url:
                # Simple parsing for health check
                url_parts = redis_url.replace('redis://', '').split(':')
                host = url_parts[0]
                port = int(url_parts[1]) if len(url_parts) > 1 else 6379
                if check_port(host, port):
                    health_status['checks']['redis'] = 'ok'
                else:
                    health_status['checks']['redis'] = 'unreachable'
                    health_status['status'] = 'unhealthy'
        except Exception as e:
            health_status['checks']['redis'] = f'error: {e}'
            health_status['status'] = 'unhealthy'
    
    # Check RabbitMQ connection if configured
    rabbitmq_url = os.getenv('RABBITMQ_URL')
    if rabbitmq_url:
        try:
            # Extract host and port from URL
            if 'amqp://' in rabbitmq_url:
                # Simple parsing for health check
                url_parts = rabbitmq_url.split('@')[1].split('/')[0]
                host, port = url_parts.split(':')
                if check_port(host, int(port)):
                    health_status['checks']['rabbitmq'] = 'ok'
                else:
                    health_status['checks']['rabbitmq'] = 'unreachable'
                    health_status['status'] = 'unhealthy'
        except Exception as e:
            health_status['checks']['rabbitmq'] = f'error: {e}'
            health_status['status'] = 'unhealthy'
    
    return health_status


def main():
    """Main health check execution."""
    try:
        health = check_service_health()
        
        # Log health status for debugging
        if health['status'] == 'healthy':
            logger.info("Health check passed")
            sys.exit(0)
        else:
            logger.error(f"Health check failed: {health}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Health check error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
