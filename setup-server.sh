#!/bin/bash

# Setup script for Mehngai Monitor on Ubuntu/Linux server
# Run as: sudo bash setup-server.sh

set -e

echo "====================================="
echo "Mehngai Monitor - Server Setup"
echo "====================================="

# Configuration
APP_USER="mehngai"
APP_HOME="/home/$APP_USER/mehngai-monitor"
REPO_PATH="/path/to/your/repo"  # UPDATE THIS TO YOUR REPO PATH

# 1. Create system user account (if doesn't exist)
echo "[1/7] Creating system user..."
if ! id "$APP_USER" &>/dev/null; then
    useradd -r -s /bin/bash -d "$APP_HOME" -m "$APP_USER"
    echo "✓ User '$APP_USER' created"
else
    echo "✓ User '$APP_USER' already exists"
fi

# 2. Copy app files
echo "[2/7] Setting up application directory..."
if [ ! -d "$APP_HOME" ]; then
    mkdir -p "$APP_HOME"
fi
cp -r "$REPO_PATH"/* "$APP_HOME"/ 2>/dev/null || true
chown -R "$APP_USER:$APP_USER" "$APP_HOME"
chmod -R 755 "$APP_HOME"

# 3. Create and activate virtual environment
echo "[3/7] Setting up Python virtual environment..."
cd "$APP_HOME"
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate venv and install dependencies
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
echo "✓ Dependencies installed"

# 4. Create necessary directories
echo "[4/7] Creating data directories..."
mkdir -p "$APP_HOME/data"
mkdir -p "$APP_HOME/images"
mkdir -p "$APP_HOME/logs"
chown -R "$APP_USER:$APP_USER" "$APP_HOME/data" "$APP_HOME/images" "$APP_HOME/logs"
chmod -R 755 "$APP_HOME/data" "$APP_HOME/images" "$APP_HOME/logs"
echo "✓ Directories created"

# 5. Verify .env file exists
echo "[5/7] Checking .env configuration..."
if [ ! -f "$APP_HOME/.env" ]; then
    echo "⚠ WARNING: .env file not found!"
    echo "  Please create $APP_HOME/.env with:"
    echo "  GEMINI_API_KEY=your_key_here"
    echo ""
else
    echo "✓ .env file found"
    chown "$APP_USER:$APP_USER" "$APP_HOME/.env"
    chmod 600 "$APP_HOME/.env"
fi

# 6. Install systemd service
echo "[6/7] Installing systemd service..."
if [ -f "$APP_HOME/mehngai-monitor.service" ]; then
    cp "$APP_HOME/mehngai-monitor.service" /etc/systemd/system/
    sed -i "s|/home/mehngai|$APP_HOME|g" /etc/systemd/system/mehngai-monitor.service
    sed -i "s|User=mehngai|User=$APP_USER|g" /etc/systemd/system/mehngai-monitor.service
    systemctl daemon-reload
    echo "✓ Service installed"
else
    echo "✗ mehngai-monitor.service not found!"
    exit 1
fi

# 7. Start the service
echo "[7/7] Starting service..."
systemctl enable mehngai-monitor
systemctl start mehngai-monitor
sleep 2

if systemctl is-active --quiet mehngai-monitor; then
    echo "✓ Service started successfully"
else
    echo "✗ Service failed to start. Check logs:"
    systemctl status mehngai-monitor
    journalctl -u mehngai-monitor -n 50
    exit 1
fi

echo ""
echo "====================================="
echo "✓ Setup Complete!"
echo "====================================="
echo ""
echo "Next steps:"
echo "1. Update .env file at: $APP_HOME/.env"
echo "2. Verify service is running:"
echo "   systemctl status mehngai-monitor"
echo "3. View logs:"
echo "   journalctl -u mehngai-monitor -f"
echo "4. Access web UI at:"
echo "   http://your-server-ip:8000"
echo ""
echo "Useful commands:"
echo "  systemctl restart mehngai-monitor"
echo "  systemctl stop mehngai-monitor"
echo "  journalctl -u mehngai-monitor -n 100 -f"
echo ""
