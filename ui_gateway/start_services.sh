#!/bin/bash
# Start both WebSocket server and Streamlit dashboard

set -e

echo "Starting UI Gateway services..."

# Function to handle shutdown
cleanup() {
    echo "Shutting down services..."
    kill $WEBSOCKET_PID $STREAMLIT_PID 2>/dev/null || true
    wait
    echo "Services stopped"
    exit 0
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

# Start WebSocket server in background
echo "Starting WebSocket server on port 8765..."
python start_websocket.py &
WEBSOCKET_PID=$!

# Wait a moment for WebSocket server to start
sleep 2

# Start Streamlit dashboard
echo "Starting Streamlit dashboard on port 8501..."
streamlit run dashboard.py --server.port=8501 --server.address=0.0.0.0 &
STREAMLIT_PID=$!

# Wait for both processes
echo "Both services started. WebSocket PID: $WEBSOCKET_PID, Streamlit PID: $STREAMLIT_PID"
echo "Press Ctrl+C to stop all services"

# Wait for either process to exit
wait -n

# If we get here, one process has exited, so clean up
cleanup
