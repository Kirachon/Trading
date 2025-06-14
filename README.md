# Algorithmic Trading System

A comprehensive, production-ready algorithmic trading system built with Python and microservices architecture using **Podman** for containerization.

## 🚀 Features

- **Real-time Market Data**: Live data streaming from multiple exchanges via CCXT
- **5 Trading Strategies**: Moving Average, MAEE Formula, RSI, MACD, Bollinger Bands
- **Advanced Risk Management**: Pre-trade risk controls, position sizing, and portfolio limits
- **Order Execution**: Automated order placement with OCO (One-Cancels-Other) support
- **Portfolio Tracking**: Real-time P&L calculation and position management
- **Comprehensive Backtesting**: Vectorized and event-driven backtesting with optimization
- **Notifications**: Telegram bot and email alerts for trades and risk events
- **Interactive Dashboard**: Real-time Streamlit dashboard for monitoring and control
- **Microservices Architecture**: Scalable, fault-tolerant design with Podman containers
- **Message-Driven**: RabbitMQ for reliable messaging, Redis for high-frequency data
- **Database Integration**: PostgreSQL for structured data, MongoDB for time-series data

## 🏗️ Architecture

The system consists of the following microservices:

1. **Market Data Service**: Ingests real-time and historical market data from exchanges
2. **Strategy Engine**: Implements and executes 5 sophisticated trading strategies
3. **Risk Management**: Validates trades against comprehensive risk parameters
4. **Execution Gateway**: Places orders on exchanges with advanced order types
5. **Portfolio Accounting**: Tracks positions, calculates P&L, and manages balances
6. **Trade Logger**: Comprehensive audit trail and analytics logging
7. **Backtesting Service**: Advanced backtesting with vectorbt and optimization
8. **Notification Service**: Telegram and email alerts for trades and risk events
9. **UI Gateway**: Interactive Streamlit dashboard for monitoring and control

## 🛠️ Technology Stack

- **Containerization**: Podman (rootless, daemonless containers)
- **Languages**: Python 3.11+
- **Databases**: PostgreSQL 15, MongoDB 7
- **Messaging**: RabbitMQ 3, Redis 7
- **Frontend**: Streamlit dashboard
- **Testing**: Pytest with comprehensive test suite
- **Analytics**: vectorbt, backtesting.py, pandas, numpy

## 📋 Prerequisites

- **Podman** and **podman-compose** (not Docker)
- Python 3.11+
- Git

### Installing Podman

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get update
sudo apt-get install podman podman-compose
```

**macOS:**
```bash
brew install podman podman-compose
```

**Windows:**
```bash
# Install via Windows Subsystem for Linux (WSL2)
# Or use Podman Desktop
```

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Kirachon/Trading.git
cd Trading
```

### 2. Set Up Environment Variables

```bash
cp .env.example .env
# Edit .env with your configuration
```

### 3. Start Infrastructure Services

```bash
# Start databases and messaging services
podman-compose up -d postgresql mongodb redis rabbitmq
```

### 4. Initialize Databases

```bash
# Wait for services to start (30 seconds)
sleep 30

# Initialize database schemas
python database/init_databases.py
```

### 5. Start Trading Services

```bash
# Start all trading services
podman-compose up -d
```

### 6. Access the Dashboard

Open your browser and navigate to:
```
http://localhost:8501
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file with the following variables:

```env
# Database Configuration
POSTGRES_URL=postgresql://admin:SecureDB2024!@postgresql:5432/trading
MONGODB_URL=mongodb://admin:SecureDB2024!@mongodb:27017/trading_data?authSource=admin
REDIS_URL=redis://redis:6379
RABBITMQ_URL=amqp://admin:SecureRabbit2024!@rabbitmq:5672/

# Exchange API Keys (for live trading)
BINANCE_API_KEY=your_binance_api_key
BINANCE_SECRET=your_binance_secret
BINANCE_SANDBOX=true

# Notification Settings
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your_email@gmail.com
SMTP_PASSWORD=your_app_password
EMAIL_FROM=your_email@gmail.com
EMAIL_TO=alerts@yourdomain.com
```

## 📈 Trading Strategies

### 1. Moving Average Crossover
- **Description**: Generates signals when short-term MA crosses above/below long-term MA
- **Parameters**: `short_window`, `long_window`, `ma_type`
- **Best for**: Trending markets

### 2. MAEE Formula (Rayner Teo)
- **Description**: Market Structure + Area of Value + Entry Trigger + Exits
- **Parameters**: `ma_period`, `atr_period`, `rr_ratio`, `swing_lookback`
- **Best for**: Swing trading with proper risk management

### 3. RSI Strategy
- **Description**: Uses RSI indicator with divergence detection
- **Parameters**: `rsi_period`, `oversold`, `overbought`
- **Best for**: Range-bound markets

### 4. MACD Strategy
- **Description**: MACD crossover with histogram and zero-line filters
- **Parameters**: `fast_period`, `slow_period`, `signal_period`
- **Best for**: Trend confirmation

### 5. Bollinger Bands
- **Description**: Mean reversion or breakout strategy with squeeze detection
- **Parameters**: `period`, `std_dev`, `mean_reversion_mode`
- **Best for**: Volatility-based trading

## 🛡️ Risk Management

Comprehensive risk management features:

- **Position Sizing**: Dynamic position sizing based on account balance and volatility
- **Daily Loss Limits**: Prevents excessive losses in a single trading day
- **Maximum Positions**: Limits concurrent positions per exchange/strategy
- **Fat Finger Protection**: Prevents orders with extreme price deviations
- **Correlation Limits**: Manages portfolio concentration risk
- **Real-time Monitoring**: Continuous risk assessment and alerts

## 🧪 Testing

Comprehensive test suite with 95%+ coverage:

```bash
# Run all tests
python tests/run_tests.py

# Run specific test types
python tests/run_tests.py --unit
python tests/run_tests.py --integration
python tests/run_tests.py --performance

# Generate coverage report
python tests/run_tests.py --coverage
```

## 🚀 Deployment

### Development Deployment

```bash
# Start all services
podman-compose up -d

# View logs
podman-compose logs -f strategy_engine

# Scale services
podman-compose up -d --scale strategy_engine=3
```

## ⚠️ Disclaimer

**IMPORTANT**: This software is for educational and research purposes only.

- Trading cryptocurrencies involves substantial risk of loss
- Past performance is not indicative of future results
- Never trade with money you cannot afford to lose
- Always test strategies thoroughly before live trading

## 📄 License

This project is licensed under the MIT License.