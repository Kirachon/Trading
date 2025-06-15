#!/bin/bash
# Optimized Container Build Script for Trading System
# Eliminates duplication and optimizes layer caching

set -e

echo "🚀 Starting optimized container build process..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Podman is available
if ! command -v podman &> /dev/null; then
    print_error "Podman is not installed or not in PATH"
    exit 1
fi

# Clean up old images to prevent bloat
print_status "Cleaning up old images..."
podman image prune -f || true

# Build base image first (shared by all services)
print_status "Building optimized base image..."
if podman build -f Containerfile.base -t localhost/trading-base:latest .; then
    print_success "Base image built successfully"
else
    print_error "Failed to build base image"
    exit 1
fi

# List of services to build
SERVICES=(
    "market_data"
    "strategy_engine"
    "risk_management"
    "execution_gateway"
    "portfolio_accounting"
    "trade_logger"
    "notifications"
    "backtesting"
    "ui_gateway"
    "outbox_publisher"
)

# Build each service
print_status "Building service images..."
for service in "${SERVICES[@]}"; do
    print_status "Building $service..."
    
    if [ -f "$service/Containerfile" ]; then
        if podman build -f "$service/Containerfile" -t "localhost/trading-$service:latest" .; then
            print_success "$service built successfully"
        else
            print_error "Failed to build $service"
            exit 1
        fi
    else
        print_warning "Containerfile not found for $service, skipping..."
    fi
done

# Show image sizes for optimization verification
print_status "Container image sizes:"
podman images --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}" | grep "localhost/trading"

# Clean up build cache
print_status "Cleaning up build cache..."
podman system prune -f || true

print_success "All containers built successfully! 🎉"
print_status "You can now run: podman-compose up -d"

# Optional: Show total disk usage
echo ""
print_status "Total Podman storage usage:"
podman system df
