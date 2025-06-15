"""
OpenTelemetry distributed tracing module for Trading System services.
Provides standardized tracing across all services for debugging and monitoring.
"""

import os
import functools
from typing import Dict, Any, Optional
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
import structlog

logger = structlog.get_logger(__name__)


class TradingTracer:
    """Centralized tracing configuration for trading services."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.tracer = None
        self._initialized = False
    
    def initialize(self):
        """Initialize OpenTelemetry tracing."""
        if self._initialized:
            return
        
        try:
            # Create resource with service information
            resource = Resource.create({
                "service.name": self.service_name,
                "service.version": "1.0.0",
                "deployment.environment": os.getenv("ENVIRONMENT", "development")
            })
            
            # Set up tracer provider
            trace.set_tracer_provider(TracerProvider(resource=resource))
            
            # Configure Jaeger exporter
            jaeger_endpoint = os.getenv("JAEGER_ENDPOINT", "http://localhost:14268/api/traces")
            jaeger_exporter = JaegerExporter(
                collector_endpoint=jaeger_endpoint,
            )
            
            # Add span processor
            span_processor = BatchSpanProcessor(jaeger_exporter)
            trace.get_tracer_provider().add_span_processor(span_processor)
            
            # Get tracer
            self.tracer = trace.get_tracer(self.service_name)
            
            # Auto-instrument common libraries
            self._setup_auto_instrumentation()
            
            self._initialized = True
            logger.info(f"Tracing initialized for service: {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize tracing: {e}")
            # Create a no-op tracer as fallback
            self.tracer = trace.NoOpTracer()
    
    def _setup_auto_instrumentation(self):
        """Set up automatic instrumentation for common libraries."""
        try:
            # Database instrumentation
            AsyncPGInstrumentor().instrument()
            PymongoInstrumentor().instrument()
            RedisInstrumentor().instrument()
            
            # HTTP client instrumentation
            AioHttpClientInstrumentor().instrument()
            
            logger.info("Auto-instrumentation configured")
            
        except Exception as e:
            logger.warning(f"Some auto-instrumentation failed: {e}")
    
    def start_span(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        """Start a new span with optional attributes."""
        if not self._initialized:
            self.initialize()
        
        span = self.tracer.start_span(name)
        
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        
        return span
    
    def trace_method(self, span_name: Optional[str] = None, attributes: Optional[Dict[str, Any]] = None):
        """Decorator to trace method execution."""
        def decorator(func):
            method_span_name = span_name or f"{self.service_name}.{func.__name__}"
            
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                if not self._initialized:
                    self.initialize()
                
                with self.tracer.start_as_current_span(method_span_name) as span:
                    # Add method attributes
                    span.set_attribute("method.name", func.__name__)
                    span.set_attribute("service.name", self.service_name)
                    
                    # Add custom attributes
                    if attributes:
                        for key, value in attributes.items():
                            span.set_attribute(key, value)
                    
                    try:
                        result = await func(*args, **kwargs)
                        span.set_attribute("method.status", "success")
                        return result
                    except Exception as e:
                        span.set_attribute("method.status", "error")
                        span.set_attribute("error.type", type(e).__name__)
                        span.set_attribute("error.message", str(e))
                        span.record_exception(e)
                        raise
            
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                if not self._initialized:
                    self.initialize()
                
                with self.tracer.start_as_current_span(method_span_name) as span:
                    # Add method attributes
                    span.set_attribute("method.name", func.__name__)
                    span.set_attribute("service.name", self.service_name)
                    
                    # Add custom attributes
                    if attributes:
                        for key, value in attributes.items():
                            span.set_attribute(key, value)
                    
                    try:
                        result = func(*args, **kwargs)
                        span.set_attribute("method.status", "success")
                        return result
                    except Exception as e:
                        span.set_attribute("method.status", "error")
                        span.set_attribute("error.type", type(e).__name__)
                        span.set_attribute("error.message", str(e))
                        span.record_exception(e)
                        raise
            
            # Return appropriate wrapper based on function type
            import asyncio
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                return sync_wrapper
        
        return decorator
    
    def trace_trading_operation(self, operation_type: str, symbol: str = None, exchange: str = None):
        """Decorator specifically for trading operations."""
        def decorator(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                span_name = f"trading.{operation_type}"
                attributes = {
                    "trading.operation": operation_type,
                    "service.name": self.service_name
                }
                
                if symbol:
                    attributes["trading.symbol"] = symbol
                if exchange:
                    attributes["trading.exchange"] = exchange
                
                with self.tracer.start_as_current_span(span_name) as span:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)
                    
                    try:
                        result = await func(*args, **kwargs)
                        span.set_attribute("trading.status", "success")
                        return result
                    except Exception as e:
                        span.set_attribute("trading.status", "error")
                        span.set_attribute("error.type", type(e).__name__)
                        span.record_exception(e)
                        raise
            
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                span_name = f"trading.{operation_type}"
                attributes = {
                    "trading.operation": operation_type,
                    "service.name": self.service_name
                }
                
                if symbol:
                    attributes["trading.symbol"] = symbol
                if exchange:
                    attributes["trading.exchange"] = exchange
                
                with self.tracer.start_as_current_span(span_name) as span:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)
                    
                    try:
                        result = func(*args, **kwargs)
                        span.set_attribute("trading.status", "success")
                        return result
                    except Exception as e:
                        span.set_attribute("trading.status", "error")
                        span.set_attribute("error.type", type(e).__name__)
                        span.record_exception(e)
                        raise
            
            # Return appropriate wrapper based on function type
            import asyncio
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                return sync_wrapper
        
        return decorator


# Global tracer instance
_tracer: Optional[TradingTracer] = None


def get_tracer(service_name: str = None) -> TradingTracer:
    """Get or create global tracer instance."""
    global _tracer
    
    if _tracer is None:
        if service_name is None:
            raise ValueError("service_name must be provided for first initialization")
        _tracer = TradingTracer(service_name)
        _tracer.initialize()
    
    return _tracer


def trace_async(span_name: str = None, attributes: Dict[str, Any] = None):
    """Convenience decorator for async functions."""
    def decorator(func):
        tracer = get_tracer()
        return tracer.trace_method(span_name, attributes)(func)
    return decorator


def trace_trading(operation_type: str, symbol: str = None, exchange: str = None):
    """Convenience decorator for trading operations."""
    def decorator(func):
        tracer = get_tracer()
        return tracer.trace_trading_operation(operation_type, symbol, exchange)(func)
    return decorator
