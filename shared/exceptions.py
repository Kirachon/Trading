"""
Custom Exception Hierarchy for Trading System

This module defines specific exception types for different components
of the trading system to enable better error handling and recovery.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class TradingSystemError(Exception):
    """Base exception for all trading system errors."""
    
    def __init__(self, message: str, error_code: str = None, context: Dict[str, Any] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.context = context or {}
        self.timestamp = datetime.now()
        
        # Log the exception with context
        logger.error(f"{self.error_code}: {message}", extra={
            'error_code': self.error_code,
            'context': self.context,
            'timestamp': self.timestamp.isoformat()
        })


# Database-related exceptions
class DatabaseError(TradingSystemError):
    """Base class for database-related errors."""
    pass


class DatabaseConnectionError(DatabaseError):
    """Database connection failed."""
    pass


class DatabaseTimeoutError(DatabaseError):
    """Database operation timed out."""
    pass


class DatabaseDeadlockError(DatabaseError):
    """Database deadlock detected."""
    pass


class DatabaseIntegrityError(DatabaseError):
    """Database integrity constraint violation."""
    pass


# Market Data exceptions
class MarketDataError(TradingSystemError):
    """Base class for market data errors."""
    pass


class MarketDataStaleError(MarketDataError):
    """Market data is too stale for trading decisions."""
    pass


class MarketDataUnavailableError(MarketDataError):
    """Market data is not available."""
    pass


class ExchangeConnectionError(MarketDataError):
    """Failed to connect to exchange."""
    pass


class RateLimitExceededError(MarketDataError):
    """Exchange rate limit exceeded."""
    pass


# Strategy-related exceptions
class StrategyError(TradingSystemError):
    """Base class for strategy errors."""
    pass


class StrategyConfigurationError(StrategyError):
    """Strategy configuration is invalid."""
    pass


class StrategyCalculationError(StrategyError):
    """Error in strategy calculations."""
    pass


class InsufficientDataError(StrategyError):
    """Insufficient data for strategy calculations."""
    pass


# Risk Management exceptions
class RiskManagementError(TradingSystemError):
    """Base class for risk management errors."""
    pass


class RiskLimitExceededError(RiskManagementError):
    """Risk limit has been exceeded."""
    pass


class FatFingerError(RiskManagementError):
    """Potential fat finger trade detected."""
    pass


class PositionSizeError(RiskManagementError):
    """Position size exceeds limits."""
    pass


class DailyLossLimitError(RiskManagementError):
    """Daily loss limit exceeded."""
    pass


class TradeFrequencyError(RiskManagementError):
    """Trade frequency limit exceeded."""
    pass


# Portfolio Management exceptions
class PortfolioError(TradingSystemError):
    """Base class for portfolio management errors."""
    pass


class InsufficientBalanceError(PortfolioError):
    """Insufficient balance for trade."""
    pass


class PositionNotFoundError(PortfolioError):
    """Position not found."""
    pass


class PortfolioCalculationError(PortfolioError):
    """Error in portfolio calculations."""
    pass


# Execution exceptions
class ExecutionError(TradingSystemError):
    """Base class for trade execution errors."""
    pass


class OrderRejectedError(ExecutionError):
    """Order was rejected by exchange."""
    pass


class OrderTimeoutError(ExecutionError):
    """Order execution timed out."""
    pass


class InsufficientLiquidityError(ExecutionError):
    """Insufficient liquidity for order."""
    pass


# Messaging exceptions
class MessagingError(TradingSystemError):
    """Base class for messaging errors."""
    pass


class MessagePublishError(MessagingError):
    """Failed to publish message."""
    pass


class MessageConsumeError(MessagingError):
    """Failed to consume message."""
    pass


class MessageSerializationError(MessagingError):
    """Failed to serialize/deserialize message."""
    pass


# Configuration exceptions
class ConfigurationError(TradingSystemError):
    """Base class for configuration errors."""
    pass


class MissingConfigurationError(ConfigurationError):
    """Required configuration is missing."""
    pass


class InvalidConfigurationError(ConfigurationError):
    """Configuration value is invalid."""
    pass


# Circuit Breaker exceptions
class CircuitBreakerError(TradingSystemError):
    """Circuit breaker is open."""
    pass


class ServiceUnavailableError(TradingSystemError):
    """Service is temporarily unavailable."""
    pass


# Validation exceptions
class ValidationError(TradingSystemError):
    """Base class for validation errors."""
    pass


class DataValidationError(ValidationError):
    """Data validation failed."""
    pass


class SignalValidationError(ValidationError):
    """Trading signal validation failed."""
    pass


# Recovery strategies
class RecoveryStrategy:
    """Base class for error recovery strategies."""
    
    @staticmethod
    def should_retry(exception: Exception, attempt: int, max_attempts: int = 3) -> bool:
        """Determine if operation should be retried."""
        if attempt >= max_attempts:
            return False
        
        # Retry on transient errors
        if isinstance(exception, (DatabaseTimeoutError, DatabaseDeadlockError, 
                                ExchangeConnectionError, RateLimitExceededError,
                                MessagePublishError, ServiceUnavailableError)):
            return True
        
        # Don't retry on permanent errors
        if isinstance(exception, (DatabaseIntegrityError, ConfigurationError,
                                ValidationError, RiskLimitExceededError)):
            return False
        
        return False
    
    @staticmethod
    def get_retry_delay(attempt: int, base_delay: float = 1.0) -> float:
        """Calculate retry delay with exponential backoff."""
        return base_delay * (2 ** attempt)
    
    @staticmethod
    def should_circuit_break(exception: Exception) -> bool:
        """Determine if circuit breaker should be triggered."""
        return isinstance(exception, (DatabaseConnectionError, ExchangeConnectionError,
                                    ServiceUnavailableError))


def handle_exception(func):
    """Decorator for standardized exception handling."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TradingSystemError:
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            # Convert generic exceptions to our hierarchy
            logger.error(f"Unhandled exception in {func.__name__}: {e}")
            raise TradingSystemError(f"Unexpected error in {func.__name__}: {str(e)}")
    
    return wrapper


async def handle_async_exception(func):
    """Decorator for standardized async exception handling."""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except TradingSystemError:
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            # Convert generic exceptions to our hierarchy
            logger.error(f"Unhandled exception in {func.__name__}: {e}")
            raise TradingSystemError(f"Unexpected error in {func.__name__}: {str(e)}")
    
    return wrapper


def log_exception_context(exception: Exception, context: Dict[str, Any] = None):
    """Log exception with additional context."""
    if isinstance(exception, TradingSystemError):
        logger.error(f"Trading system error: {exception.message}", extra={
            'error_code': exception.error_code,
            'context': {**exception.context, **(context or {})},
            'timestamp': exception.timestamp.isoformat()
        })
    else:
        logger.error(f"Unexpected error: {str(exception)}", extra={
            'context': context or {},
            'exception_type': type(exception).__name__
        })
