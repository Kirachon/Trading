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

-- Insert default strategy configurations
INSERT INTO strategy_configs (strategy_id, strategy_name, parameters) VALUES
('ma_crossover', 'Moving Average Crossover', '{"short_window": 50, "long_window": 200}'),
('maee_formula', 'MAEE Formula', '{"ma_period": 200, "atr_period": 14, "rr_ratio": 2.0, "swing_lookback": 20}'),
('rsi_strategy', 'RSI Strategy', '{"rsi_period": 14, "oversold": 30, "overbought": 70}'),
('macd_strategy', 'MACD Strategy', '{"fast_period": 12, "slow_period": 26, "signal_period": 9}'),
('bollinger_bands', 'Bollinger Bands', '{"period": 20, "std_dev": 2}')
ON CONFLICT (strategy_id) DO NOTHING;

-- Insert default risk limits
INSERT INTO risk_limits (exchange_id, symbol, max_position_size, max_daily_loss, max_trades_per_day, fat_finger_threshold) VALUES
('binance', NULL, 10000.00, 1000.00, 50, 0.05),
('kucoin', NULL, 10000.00, 1000.00, 50, 0.05)
ON CONFLICT DO NOTHING;

-- Insert initial portfolio balances (example)
INSERT INTO portfolio (exchange_id, asset, balance) VALUES
('binance', 'USDT', 100000.00),
('binance', 'BTC', 0.00),
('binance', 'ETH', 0.00)
ON CONFLICT (exchange_id, asset) DO NOTHING;

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
