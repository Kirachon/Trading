# Deployment Guide

This guide covers deploying the Algorithmic Trading System using Podman in various environments.

## Prerequisites

### System Requirements

- **CPU**: 4+ cores recommended
- **RAM**: 8GB minimum, 16GB recommended
- **Storage**: 50GB+ SSD storage
- **Network**: Stable internet connection with low latency to exchanges

### Software Requirements

- **Podman**: 4.0+ (rootless, daemonless container runtime)
- **podman-compose**: 1.0+
- **Python**: 3.11+
- **Git**: Latest version

## Installation

### 1. Install Podman

#### Linux (Ubuntu/Debian)
```bash
# Update package list
sudo apt-get update

# Install Podman and compose
sudo apt-get install -y podman podman-compose

# Enable user lingering (for systemd services)
sudo loginctl enable-linger $USER

# Configure Podman for rootless operation
echo 'unqualified-search-registries = ["docker.io"]' | sudo tee /etc/containers/registries.conf
```

#### CentOS/RHEL/Fedora
```bash
# Install Podman
sudo dnf install -y podman podman-compose

# Enable user lingering
sudo loginctl enable-linger $USER
```

#### macOS
```bash
# Install via Homebrew
brew install podman podman-compose

# Initialize Podman machine
podman machine init
podman machine start
```

#### Windows
```bash
# Install via WSL2 or use Podman Desktop
# Follow official Podman documentation for Windows setup
```

### 2. Verify Installation

```bash
# Check Podman version
podman --version

# Check podman-compose version
podman-compose --version

# Test Podman functionality
podman run hello-world
```

## Development Deployment

### 1. Clone Repository

```bash
git clone https://github.com/Kirachon/Trading.git
cd Trading
```

### 2. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit configuration
nano .env
```

### 3. Start Infrastructure Services

```bash
# Start databases and messaging
podman-compose up -d postgresql mongodb redis rabbitmq

# Wait for services to initialize
sleep 30

# Check service status
podman-compose ps
```

### 4. Initialize Databases

```bash
# Run database initialization
python database/init_databases.py

# Verify database setup
podman-compose exec postgresql psql -U admin -d trading -c "\dt"
```

### 5. Start Trading Services

```bash
# Start all trading services
podman-compose up -d

# Monitor logs
podman-compose logs -f
```

### 6. Verify Deployment

```bash
# Check all services are running
podman-compose ps

# Access dashboard
curl http://localhost:8501

# Check service health
podman-compose exec market_data python -c "print('Market Data Service OK')"
```

## Production Deployment

### 1. Security Hardening

#### Update System
```bash
# Update all packages
sudo apt-get update && sudo apt-get upgrade -y

# Install security updates
sudo unattended-upgrades
```

#### Configure Firewall
```bash
# Install UFW
sudo apt-get install ufw

# Configure firewall rules
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow ssh
sudo ufw allow 8501/tcp  # Dashboard
sudo ufw enable
```

#### Secure Database Access
```bash
# Generate strong passwords
openssl rand -base64 32  # For PostgreSQL
openssl rand -base64 32  # For MongoDB
openssl rand -base64 32  # For RabbitMQ
openssl rand -base64 32  # For Redis
```

### 2. Production Configuration

#### Environment Variables
```bash
# Production .env file
cat > .env << EOF
# Production Database URLs
POSTGRES_URL=postgresql://admin:$(openssl rand -base64 32)@postgresql:5432/trading
MONGODB_URL=mongodb://admin:$(openssl rand -base64 32)@mongodb:27017/trading_data?authSource=admin
REDIS_URL=redis://redis:6379
RABBITMQ_URL=amqp://admin:$(openssl rand -base64 32)@rabbitmq:5672/

# Exchange API Keys (LIVE TRADING)
BINANCE_API_KEY=your_live_api_key
BINANCE_SECRET=your_live_secret
BINANCE_SANDBOX=false

# Notification Settings
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your_email@gmail.com
SMTP_PASSWORD=your_app_password

# Production Settings
ENABLE_LIVE_TRADING=true
LOG_LEVEL=INFO
DEBUG=false
EOF
```

#### Production Compose File
```bash
# Create production compose file
cat > podman-compose.prod.yml << EOF
version: '3.8'

services:
  # Infrastructure services with production settings
  postgresql:
    image: postgres:15
    environment:
      POSTGRES_DB: trading
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: \${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./database/init.sql:/docker-entrypoint-initdb.d/init.sql
    restart: always
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'
    networks:
      - trading_net

  # Add other services with production configurations...
EOF
```

### 3. SSL/TLS Configuration

#### Generate SSL Certificates
```bash
# Install certbot
sudo apt-get install certbot

# Generate certificates (for domain-based deployment)
sudo certbot certonly --standalone -d your-domain.com

# Or use self-signed certificates for internal use
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes
```

#### Configure Reverse Proxy
```bash
# Install nginx
sudo apt-get install nginx

# Configure nginx for SSL termination
cat > /etc/nginx/sites-available/trading << EOF
server {
    listen 443 ssl;
    server_name your-domain.com;
    
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    location / {
        proxy_pass http://localhost:8501;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

# Enable site
sudo ln -s /etc/nginx/sites-available/trading /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

### 4. Monitoring and Logging

#### Configure Log Rotation
```bash
# Create logrotate configuration
cat > /etc/logrotate.d/trading << EOF
/var/log/trading/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 trading trading
    postrotate
        podman-compose restart trade_logger
    endscript
}
EOF
```

#### Set Up Health Checks
```bash
# Create health check script
cat > /usr/local/bin/trading-health-check.sh << EOF
#!/bin/bash
set -e

# Check if all services are running
podman-compose ps | grep -q "Up" || exit 1

# Check database connectivity
podman-compose exec -T postgresql pg_isready -U admin || exit 1

# Check Redis connectivity
podman-compose exec -T redis redis-cli ping | grep -q "PONG" || exit 1

# Check RabbitMQ connectivity
podman-compose exec -T rabbitmq rabbitmqctl status || exit 1

echo "All services healthy"
EOF

chmod +x /usr/local/bin/trading-health-check.sh
```

### 5. Backup and Recovery

#### Database Backup Script
```bash
# Create backup script
cat > /usr/local/bin/backup-trading.sh << EOF
#!/bin/bash
BACKUP_DIR="/backups/trading"
DATE=\$(date +%Y%m%d_%H%M%S)

mkdir -p \$BACKUP_DIR

# Backup PostgreSQL
podman-compose exec -T postgresql pg_dump -U admin trading > \$BACKUP_DIR/postgres_\$DATE.sql

# Backup MongoDB
podman-compose exec -T mongodb mongodump --host localhost --port 27017 --out \$BACKUP_DIR/mongo_\$DATE

# Compress backups
tar -czf \$BACKUP_DIR/trading_backup_\$DATE.tar.gz \$BACKUP_DIR/*_\$DATE*

# Clean up old backups (keep 7 days)
find \$BACKUP_DIR -name "*.tar.gz" -mtime +7 -delete

echo "Backup completed: \$BACKUP_DIR/trading_backup_\$DATE.tar.gz"
EOF

chmod +x /usr/local/bin/backup-trading.sh
```

#### Schedule Backups
```bash
# Add to crontab
(crontab -l 2>/dev/null; echo "0 2 * * * /usr/local/bin/backup-trading.sh") | crontab -
```

### 6. Systemd Service Configuration

#### Create Systemd Service
```bash
# Create service file
cat > ~/.config/systemd/user/trading.service << EOF
[Unit]
Description=Algorithmic Trading System
Requires=podman.socket
After=podman.socket

[Service]
Type=oneshot
RemainAfterExit=true
WorkingDirectory=/home/\$USER/Trading
ExecStart=/usr/bin/podman-compose up -d
ExecStop=/usr/bin/podman-compose down
TimeoutStartSec=300

[Install]
WantedBy=default.target
EOF

# Enable and start service
systemctl --user daemon-reload
systemctl --user enable trading.service
systemctl --user start trading.service
```

## Scaling and High Availability

### 1. Horizontal Scaling

```bash
# Scale specific services
podman-compose up -d --scale strategy_engine=3 --scale execution_gateway=2

# Load balance with nginx
# Configure nginx upstream for multiple instances
```

### 2. Database Clustering

#### PostgreSQL High Availability
```bash
# Set up PostgreSQL streaming replication
# Configure primary-replica setup
# Use connection pooling with PgBouncer
```

#### MongoDB Replica Set
```bash
# Configure MongoDB replica set
# Set up automatic failover
# Configure read preferences
```

### 3. Message Queue Clustering

```bash
# Set up RabbitMQ cluster
# Configure Redis Sentinel for high availability
# Implement message persistence
```

## Troubleshooting

### Common Issues

#### Podman Permission Issues
```bash
# Fix subuid/subgid mappings
sudo usermod --add-subuids 100000-165535 --add-subgids 100000-165535 $USER

# Restart user session
sudo loginctl terminate-user $USER
```

#### Container Networking Issues
```bash
# Reset Podman network
podman system reset --force

# Recreate network
podman network create trading_net
```

#### Database Connection Issues
```bash
# Check database logs
podman-compose logs postgresql

# Test connectivity
podman-compose exec postgresql psql -U admin -d trading -c "SELECT 1;"
```

### Performance Optimization

#### Container Resource Limits
```bash
# Set memory and CPU limits in compose file
deploy:
  resources:
    limits:
      memory: 1G
      cpus: '0.5'
    reservations:
      memory: 512M
      cpus: '0.25'
```

#### Database Tuning
```bash
# PostgreSQL optimization
# Adjust shared_buffers, work_mem, maintenance_work_mem
# Configure connection pooling

# MongoDB optimization
# Set appropriate cache size
# Configure index strategies
```

## Security Best Practices

1. **Use strong, unique passwords** for all services
2. **Enable SSL/TLS** for all external connections
3. **Regularly update** all components and dependencies
4. **Monitor logs** for suspicious activity
5. **Implement network segmentation** with firewalls
6. **Use secrets management** for API keys and passwords
7. **Regular security audits** and penetration testing
8. **Backup encryption** for sensitive data

## Maintenance

### Regular Tasks

1. **Daily**: Check service health and logs
2. **Weekly**: Review performance metrics and alerts
3. **Monthly**: Update dependencies and security patches
4. **Quarterly**: Full system backup and disaster recovery testing

### Update Procedure

```bash
# 1. Backup current system
/usr/local/bin/backup-trading.sh

# 2. Pull latest changes
git pull origin main

# 3. Update containers
podman-compose pull

# 4. Restart services
podman-compose down
podman-compose up -d

# 5. Verify deployment
/usr/local/bin/trading-health-check.sh
```
