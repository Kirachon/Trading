"""
Shared Prometheus metrics module for Trading System services.
Provides standardized metrics collection across all services.
"""

import time
import functools
from typing import Dict, Any, Optional
from prometheus_client import Counter, Histogram, Gauge, Info, start_http_server
import structlog

logger = structlog.get_logger(__name__)

# Common metrics for all services
REQUEST_COUNT = Counter(
    'trading_requests_total',
    'Total number of requests processed',
    ['service', 'method', 'status']
)

REQUEST_DURATION = Histogram(
    'trading_request_duration_seconds',
    'Request processing duration in seconds',
    ['service', 'method']
)

ERROR_COUNT = Counter(
    'trading_errors_total',
    'Total number of errors',
    ['service', 'error_type']
)

ACTIVE_CONNECTIONS = Gauge(
    'trading_active_connections',
    'Number of active connections',
    ['service', 'connection_type']
)

SERVICE_INFO = Info(
    'trading_service_info',
    'Service information'
)

# Trading-specific metrics
TRADES_PROCESSED = Counter(
    'trading_trades_processed_total',
    'Total number of trades processed',
    ['service', 'exchange', 'symbol']
)

PORTFOLIO_VALUE = Gauge(
    'trading_portfolio_value_usd',
    'Current portfolio value in USD',
    ['exchange', 'strategy']
)

POSITIONS_COUNT = Gauge(
    'trading_positions_count',
    'Number of open positions',
    ['exchange', 'strategy']
)

MARKET_DATA_LATENCY = Histogram(
    'trading_market_data_latency_seconds',
    'Market data processing latency',
    ['exchange', 'symbol']
)

RISK_VIOLATIONS = Counter(
    'trading_risk_violations_total',
    'Total number of risk violations',
    ['violation_type', 'severity']
)

ORDER_EXECUTION_TIME = Histogram(
    'trading_order_execution_seconds',
    'Order execution time',
    ['exchange', 'order_type']
)

STRATEGY_SIGNALS = Counter(
    'trading_strategy_signals_total',
    'Total number of strategy signals generated',
    ['strategy', 'signal_type']
)

WEBSOCKET_CONNECTIONS = Gauge(
    'trading_websocket_connections',
    'Number of active WebSocket connections',
    ['service']
)


class MetricsCollector:
    """Centralized metrics collector for trading services."""
    
    def __init__(self, service_name: str, port: int = 8000):
        self.service_name = service_name
        self.port = port
        self.server_started = False
        
        # Set service info
        SERVICE_INFO.info({
            'service': service_name,
            'version': '1.0.0',
            'start_time': str(int(time.time()))
        })
    
    def start_metrics_server(self):
        """Start Prometheus metrics HTTP server."""
        if not self.server_started:
            try:
                start_http_server(self.port)
                self.server_started = True
                logger.info(f"Metrics server started on port {self.port}")
            except Exception as e:
                logger.error(f"Failed to start metrics server: {e}")
    
    def record_request(self, method: str, status: str = 'success'):
        """Record a request metric."""
        REQUEST_COUNT.labels(
            service=self.service_name,
            method=method,
            status=status
        ).inc()
    
    def record_error(self, error_type: str):
        """Record an error metric."""
        ERROR_COUNT.labels(
            service=self.service_name,
            error_type=error_type
        ).inc()
    
    def set_active_connections(self, connection_type: str, count: int):
        """Set active connections gauge."""
        ACTIVE_CONNECTIONS.labels(
            service=self.service_name,
            connection_type=connection_type
        ).set(count)
    
    def record_trade(self, exchange: str, symbol: str):
        """Record a trade processed."""
        TRADES_PROCESSED.labels(
            service=self.service_name,
            exchange=exchange,
            symbol=symbol
        ).inc()
    
    def set_portfolio_value(self, exchange: str, strategy: str, value: float):
        """Set portfolio value gauge."""
        PORTFOLIO_VALUE.labels(
            exchange=exchange,
            strategy=strategy
        ).set(value)
    
    def set_positions_count(self, exchange: str, strategy: str, count: int):
        """Set positions count gauge."""
        POSITIONS_COUNT.labels(
            exchange=exchange,
            strategy=strategy
        ).set(count)
    
    def record_market_data_latency(self, exchange: str, symbol: str, latency: float):
        """Record market data processing latency."""
        MARKET_DATA_LATENCY.labels(
            exchange=exchange,
            symbol=symbol
        ).observe(latency)
    
    def record_risk_violation(self, violation_type: str, severity: str = 'medium'):
        """Record a risk violation."""
        RISK_VIOLATIONS.labels(
            violation_type=violation_type,
            severity=severity
        ).inc()
    
    def record_order_execution(self, exchange: str, order_type: str, duration: float):
        """Record order execution time."""
        ORDER_EXECUTION_TIME.labels(
            exchange=exchange,
            order_type=order_type
        ).observe(duration)
    
    def record_strategy_signal(self, strategy: str, signal_type: str):
        """Record a strategy signal."""
        STRATEGY_SIGNALS.labels(
            strategy=strategy,
            signal_type=signal_type
        ).inc()
    
    def set_websocket_connections(self, count: int):
        """Set WebSocket connections count."""
        WEBSOCKET_CONNECTIONS.labels(
            service=self.service_name
        ).set(count)


def metrics_timer(service_name: str, method_name: str):
    """Decorator to time method execution and record metrics."""
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                REQUEST_COUNT.labels(
                    service=service_name,
                    method=method_name,
                    status='success'
                ).inc()
                return result
            except Exception as e:
                REQUEST_COUNT.labels(
                    service=service_name,
                    method=method_name,
                    status='error'
                ).inc()
                ERROR_COUNT.labels(
                    service=service_name,
                    error_type=type(e).__name__
                ).inc()
                raise
            finally:
                duration = time.time() - start_time
                REQUEST_DURATION.labels(
                    service=service_name,
                    method=method_name
                ).observe(duration)
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                REQUEST_COUNT.labels(
                    service=service_name,
                    method=method_name,
                    status='success'
                ).inc()
                return result
            except Exception as e:
                REQUEST_COUNT.labels(
                    service=service_name,
                    method=method_name,
                    status='error'
                ).inc()
                ERROR_COUNT.labels(
                    service=service_name,
                    error_type=type(e).__name__
                ).inc()
                raise
            finally:
                duration = time.time() - start_time
                REQUEST_DURATION.labels(
                    service=service_name,
                    method=method_name
                ).observe(duration)
        
        # Return appropriate wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# Global metrics collector instance
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector(service_name: str = None, port: int = 8000) -> MetricsCollector:
    """Get or create global metrics collector instance."""
    global _metrics_collector
    
    if _metrics_collector is None:
        if service_name is None:
            raise ValueError("service_name must be provided for first initialization")
        _metrics_collector = MetricsCollector(service_name, port)
    
    return _metrics_collector
