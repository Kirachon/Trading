-- Trading System Database Schema
-- PostgreSQL initialization script

-- Create trades table
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    exchange_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    order_id VARCHAR(100) NOT NULL UNIQUE,
    side VARCHAR(10) NOT NULL CHECK (side IN ('buy', 'sell')),
    quantity DECIMAL(18, 8) NOT NULL CHECK (quantity > 0),
    price DECIMAL(18, 8) NOT NULL CHECK (price > 0),
    timestamp BIGINT NOT NULL,
    strategy_id VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('open', 'filled', 'canceled', 'partial')),
    commission DECIMAL(18, 8) DEFAULT 0,
    realized_pnl DECIMAL(18, 8) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create positions table
CREATE TABLE IF NOT EXISTS positions (
    id SERIAL PRIMARY KEY,
    exchange_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    side VARCHAR(10) NOT NULL CHECK (side IN ('long', 'short')),
    quantity DECIMAL(18, 8) NOT NULL,
    avg_price DECIMAL(18, 8) NOT NULL CHECK (avg_price > 0),
    unrealized_pnl DECIMAL(18, 8) DEFAULT 0,
    realized_pnl DECIMAL(18, 8) DEFAULT 0,
    strategy_id VARCHAR(50) NOT NULL,
    opened_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(exchange_id, symbol, strategy_id)
);

-- Create portfolio table
CREATE TABLE IF NOT EXISTS portfolio (
    id SERIAL PRIMARY KEY,
    exchange_id VARCHAR(50) NOT NULL,
    asset VARCHAR(20) NOT NULL,
    balance DECIMAL(18, 8) NOT NULL DEFAULT 0,
    locked DECIMAL(18, 8) NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(exchange_id, asset)
);

-- Create strategy_configs table
CREATE TABLE IF NOT EXISTS strategy_configs (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(50) NOT NULL UNIQUE,
    strategy_name VARCHAR(100) NOT NULL,
    parameters JSONB NOT NULL,
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create risk_limits table
CREATE TABLE IF NOT EXISTS risk_limits (
    id SERIAL PRIMARY KEY,
    exchange_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(50),
    max_position_size DECIMAL(18, 8),
    max_daily_loss DECIMAL(18, 8),
    max_trades_per_day INTEGER,
    fat_finger_threshold DECIMAL(5, 4),
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create performance_metrics table
CREATE TABLE IF NOT EXISTS performance_metrics (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    total_trades INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    losing_trades INTEGER DEFAULT 0,
    total_pnl DECIMAL(18, 8) DEFAULT 0,
    max_drawdown DECIMAL(18, 8) DEFAULT 0,
    sharpe_ratio DECIMAL(10, 6),
    win_rate DECIMAL(5, 4),
    avg_win DECIMAL(18, 8),
    avg_loss DECIMAL(18, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(strategy_id, date)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_order_id ON trades(order_id);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy_id);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);

CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
CREATE INDEX IF NOT EXISTS idx_positions_strategy ON positions(strategy_id);
CREATE INDEX IF NOT EXISTS idx_positions_exchange ON positions(exchange_id);

CREATE INDEX IF NOT EXISTS idx_portfolio_exchange ON portfolio(exchange_id);
CREATE INDEX IF NOT EXISTS idx_portfolio_asset ON portfolio(asset);

CREATE INDEX IF NOT EXISTS idx_performance_strategy ON performance_metrics(strategy_id);
CREATE INDEX IF NOT EXISTS idx_performance_date ON performance_metrics(date);

-- Create configuration audit table
CREATE TABLE IF NOT EXISTS config_audit (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    operation VARCHAR(20) NOT NULL,
    old_values JSONB,
    new_values JSONB,
    conflict_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50) DEFAULT 'system'
);

-- Create configuration validation function
CREATE OR REPLACE FUNCTION validate_and_insert_strategy_config(
    p_strategy_id VARCHAR(50),
    p_strategy_name VARCHAR(100),
    p_parameters JSONB
) RETURNS BOOLEAN AS $$
DECLARE
    existing_config RECORD;
    validation_errors TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Validate parameters structure
    IF NOT (p_parameters ? 'short_window' OR p_parameters ? 'ma_period' OR p_parameters ? 'rsi_period' OR p_parameters ? 'fast_period' OR p_parameters ? 'period') THEN
        validation_errors := array_append(validation_errors, 'Missing required parameter configuration');
    END IF;

    -- Check for existing configuration
    SELECT * INTO existing_config FROM strategy_configs WHERE strategy_id = p_strategy_id;

    IF FOUND THEN
        -- Log conflict with details
        INSERT INTO config_audit (table_name, operation, old_values, new_values, conflict_reason)
        VALUES (
            'strategy_configs',
            'INSERT_CONFLICT',
            row_to_json(existing_config)::jsonb,
            jsonb_build_object('strategy_id', p_strategy_id, 'strategy_name', p_strategy_name, 'parameters', p_parameters),
            'Strategy configuration already exists: ' || p_strategy_id
        );

        RAISE WARNING 'Strategy configuration already exists: %', p_strategy_id;
        RETURN FALSE;
    END IF;

    -- Validate and insert
    IF array_length(validation_errors, 1) > 0 THEN
        INSERT INTO config_audit (table_name, operation, new_values, conflict_reason)
        VALUES (
            'strategy_configs',
            'INSERT_VALIDATION_FAILED',
            jsonb_build_object('strategy_id', p_strategy_id, 'strategy_name', p_strategy_name, 'parameters', p_parameters),
            array_to_string(validation_errors, '; ')
        );

        RAISE EXCEPTION 'Strategy configuration validation failed: %', array_to_string(validation_errors, '; ');
    END IF;

    -- Insert with audit
    INSERT INTO strategy_configs (strategy_id, strategy_name, parameters)
    VALUES (p_strategy_id, p_strategy_name, p_parameters);

    INSERT INTO config_audit (table_name, operation, new_values)
    VALUES (
        'strategy_configs',
        'INSERT_SUCCESS',
        jsonb_build_object('strategy_id', p_strategy_id, 'strategy_name', p_strategy_name, 'parameters', p_parameters)
    );

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Insert default strategy configurations with validation
DO $$
BEGIN
    PERFORM validate_and_insert_strategy_config('ma_crossover', 'Moving Average Crossover', '{"short_window": 50, "long_window": 200}');
    PERFORM validate_and_insert_strategy_config('maee_formula', 'MAEE Formula', '{"ma_period": 200, "atr_period": 14, "rr_ratio": 2.0, "swing_lookback": 20}');
    PERFORM validate_and_insert_strategy_config('rsi_strategy', 'RSI Strategy', '{"rsi_period": 14, "oversold": 30, "overbought": 70}');
    PERFORM validate_and_insert_strategy_config('macd_strategy', 'MACD Strategy', '{"fast_period": 12, "slow_period": 26, "signal_period": 9}');
    PERFORM validate_and_insert_strategy_config('bollinger_bands', 'Bollinger Bands', '{"period": 20, "std_dev": 2}');
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Some strategy configurations may have failed to insert. Check config_audit table for details.';
END $$;

-- Create risk limits validation function
CREATE OR REPLACE FUNCTION validate_and_insert_risk_limits(
    p_exchange_id VARCHAR(50),
    p_symbol VARCHAR(50),
    p_max_position_size DECIMAL(18, 8),
    p_max_daily_loss DECIMAL(18, 8),
    p_max_trades_per_day INTEGER,
    p_fat_finger_threshold DECIMAL(5, 4)
) RETURNS BOOLEAN AS $$
DECLARE
    existing_limit RECORD;
    validation_errors TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Validate parameters
    IF p_max_position_size <= 0 THEN
        validation_errors := array_append(validation_errors, 'max_position_size must be positive');
    END IF;

    IF p_max_daily_loss <= 0 THEN
        validation_errors := array_append(validation_errors, 'max_daily_loss must be positive');
    END IF;

    IF p_max_trades_per_day <= 0 THEN
        validation_errors := array_append(validation_errors, 'max_trades_per_day must be positive');
    END IF;

    IF p_fat_finger_threshold <= 0 OR p_fat_finger_threshold > 1 THEN
        validation_errors := array_append(validation_errors, 'fat_finger_threshold must be between 0 and 1');
    END IF;

    -- Check for existing configuration (considering NULL symbol as global)
    SELECT * INTO existing_limit FROM risk_limits
    WHERE exchange_id = p_exchange_id
    AND (symbol = p_symbol OR (symbol IS NULL AND p_symbol IS NULL));

    IF FOUND THEN
        -- Log conflict with details
        INSERT INTO config_audit (table_name, operation, old_values, new_values, conflict_reason)
        VALUES (
            'risk_limits',
            'INSERT_CONFLICT',
            row_to_json(existing_limit)::jsonb,
            jsonb_build_object(
                'exchange_id', p_exchange_id,
                'symbol', p_symbol,
                'max_position_size', p_max_position_size,
                'max_daily_loss', p_max_daily_loss,
                'max_trades_per_day', p_max_trades_per_day,
                'fat_finger_threshold', p_fat_finger_threshold
            ),
            'Risk limit already exists for exchange: ' || p_exchange_id || COALESCE(' symbol: ' || p_symbol, ' (global)')
        );

        RAISE WARNING 'Risk limit already exists for exchange: % symbol: %', p_exchange_id, COALESCE(p_symbol, 'global');
        RETURN FALSE;
    END IF;

    -- Validate and insert
    IF array_length(validation_errors, 1) > 0 THEN
        INSERT INTO config_audit (table_name, operation, new_values, conflict_reason)
        VALUES (
            'risk_limits',
            'INSERT_VALIDATION_FAILED',
            jsonb_build_object(
                'exchange_id', p_exchange_id,
                'symbol', p_symbol,
                'max_position_size', p_max_position_size,
                'max_daily_loss', p_max_daily_loss,
                'max_trades_per_day', p_max_trades_per_day,
                'fat_finger_threshold', p_fat_finger_threshold
            ),
            array_to_string(validation_errors, '; ')
        );

        RAISE EXCEPTION 'Risk limit validation failed: %', array_to_string(validation_errors, '; ');
    END IF;

    -- Insert with audit
    INSERT INTO risk_limits (exchange_id, symbol, max_position_size, max_daily_loss, max_trades_per_day, fat_finger_threshold)
    VALUES (p_exchange_id, p_symbol, p_max_position_size, p_max_daily_loss, p_max_trades_per_day, p_fat_finger_threshold);

    INSERT INTO config_audit (table_name, operation, new_values)
    VALUES (
        'risk_limits',
        'INSERT_SUCCESS',
        jsonb_build_object(
            'exchange_id', p_exchange_id,
            'symbol', p_symbol,
            'max_position_size', p_max_position_size,
            'max_daily_loss', p_max_daily_loss,
            'max_trades_per_day', p_max_trades_per_day,
            'fat_finger_threshold', p_fat_finger_threshold
        )
    );

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Insert default risk limits with validation
DO $$
BEGIN
    PERFORM validate_and_insert_risk_limits('binance', NULL, 10000.00, 1000.00, 50, 0.05);
    PERFORM validate_and_insert_risk_limits('kucoin', NULL, 10000.00, 1000.00, 50, 0.05);
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Some risk limits may have failed to insert. Check config_audit table for details.';
END $$;

-- Create portfolio validation function
CREATE OR REPLACE FUNCTION validate_and_insert_portfolio(
    p_exchange_id VARCHAR(50),
    p_asset VARCHAR(20),
    p_balance DECIMAL(18, 8)
) RETURNS BOOLEAN AS $$
DECLARE
    existing_portfolio RECORD;
    validation_errors TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Validate parameters
    IF p_balance < 0 THEN
        validation_errors := array_append(validation_errors, 'balance cannot be negative');
    END IF;

    IF LENGTH(p_asset) = 0 THEN
        validation_errors := array_append(validation_errors, 'asset cannot be empty');
    END IF;

    -- Check for existing portfolio entry
    SELECT * INTO existing_portfolio FROM portfolio
    WHERE exchange_id = p_exchange_id AND asset = p_asset;

    IF FOUND THEN
        -- Log conflict with details
        INSERT INTO config_audit (table_name, operation, old_values, new_values, conflict_reason)
        VALUES (
            'portfolio',
            'INSERT_CONFLICT',
            row_to_json(existing_portfolio)::jsonb,
            jsonb_build_object('exchange_id', p_exchange_id, 'asset', p_asset, 'balance', p_balance),
            'Portfolio entry already exists for exchange: ' || p_exchange_id || ' asset: ' || p_asset
        );

        RAISE WARNING 'Portfolio entry already exists for exchange: % asset: %', p_exchange_id, p_asset;
        RETURN FALSE;
    END IF;

    -- Validate and insert
    IF array_length(validation_errors, 1) > 0 THEN
        INSERT INTO config_audit (table_name, operation, new_values, conflict_reason)
        VALUES (
            'portfolio',
            'INSERT_VALIDATION_FAILED',
            jsonb_build_object('exchange_id', p_exchange_id, 'asset', p_asset, 'balance', p_balance),
            array_to_string(validation_errors, '; ')
        );

        RAISE EXCEPTION 'Portfolio validation failed: %', array_to_string(validation_errors, '; ');
    END IF;

    -- Insert with audit
    INSERT INTO portfolio (exchange_id, asset, balance)
    VALUES (p_exchange_id, p_asset, p_balance);

    INSERT INTO config_audit (table_name, operation, new_values)
    VALUES (
        'portfolio',
        'INSERT_SUCCESS',
        jsonb_build_object('exchange_id', p_exchange_id, 'asset', p_asset, 'balance', p_balance)
    );

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Insert initial portfolio balances with validation
DO $$
BEGIN
    PERFORM validate_and_insert_portfolio('binance', 'USDT', 100000.00);
    PERFORM validate_and_insert_portfolio('binance', 'BTC', 0.00);
    PERFORM validate_and_insert_portfolio('binance', 'ETH', 0.00);
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Some portfolio entries may have failed to insert. Check config_audit table for details.';
END $$;

-- Create daily performance tracking table
CREATE TABLE IF NOT EXISTS daily_performance (
    id SERIAL PRIMARY KEY,
    strategy_id VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    trade_count INTEGER DEFAULT 0,
    total_pnl DECIMAL(18, 8) DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    losing_trades INTEGER DEFAULT 0,
    avg_win DECIMAL(18, 8) DEFAULT 0,
    avg_loss DECIMAL(18, 8) DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(strategy_id, date)
);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_trades_updated_at BEFORE UPDATE ON trades
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_positions_updated_at BEFORE UPDATE ON positions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_portfolio_updated_at BEFORE UPDATE ON portfolio
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_strategy_configs_updated_at BEFORE UPDATE ON strategy_configs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_risk_limits_updated_at BEFORE UPDATE ON risk_limits
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
