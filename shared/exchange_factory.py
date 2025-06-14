"""
Exchange Factory for creating CCXT exchange instances with secure credentials.
"""
import ccxt.async_support as ccxt
import os
import json
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class ExchangeFactory:
    """Factory for creating and managing exchange instances."""
    
    def __init__(self):
        self._instances = {}
        
    async def get_exchange_instance(self, exchange_id: str, use_testnet: bool = True) -> ccxt.Exchange:
        """
        Creates a CCXT exchange instance with secure credentials.
        
        Args:
            exchange_id: Exchange identifier (e.g., 'binance').
            use_testnet: Whether to use the exchange's testnet.
            
        Returns:
            Configured CCXT exchange instance.
        """
        cache_key = f"{exchange_id}_{use_testnet}"
        
        if cache_key in self._instances:
            return self._instances[cache_key]
            
        try:
            # For local development, use environment variables
            # In production, this would use AWS Secrets Manager
            api_key = os.getenv(f"{exchange_id.upper()}_API_KEY")
            secret_key = os.getenv(f"{exchange_id.upper()}_SECRET_KEY")
            
            if not api_key or not secret_key:
                # Use demo credentials for testnet
                if use_testnet and exchange_id == 'binance':
                    api_key = "demo_api_key"
                    secret_key = "demo_secret_key"
                else:
                    raise ValueError(f"Credentials for {exchange_id} not found in environment variables")
            
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class({
                'apiKey': api_key,
                'secret': secret_key,
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'spot',
                    'adjustForTimeDifference': True,
                },
            })
            
            if use_testnet and exchange.has.get('test'):
                exchange.set_sandbox_mode(True)
                logger.info(f"Exchange {exchange_id} set to testnet mode")
            
            # Test connection
            await exchange.load_markets()
            logger.info(f"Successfully connected to {exchange_id}")
            
            self._instances[cache_key] = exchange
            return exchange
            
        except Exception as e:
            logger.error(f"Failed to create exchange instance for {exchange_id}: {e}")
            raise
    
    async def close_all(self):
        """Close all exchange connections."""
        for exchange in self._instances.values():
            try:
                await exchange.close()
            except Exception as e:
                logger.error(f"Error closing exchange: {e}")
        self._instances.clear()

# Global exchange factory instance
exchange_factory = ExchangeFactory()

async def get_exchange(exchange_id: str, use_testnet: bool = True) -> ccxt.Exchange:
    """Convenience function to get exchange instance."""
    return await exchange_factory.get_exchange_instance(exchange_id, use_testnet)
