# Mehngai Monitor - 24/7 Deployment Guide

This guide will help you set up Mehngai Monitor to run 24/7 on an Ubuntu/Linux server.

## Prerequisites

- Ubuntu 20.04 LTS or newer
- Python 3.8+
- sudo access
- Git (to clone the repo)

## Quick Setup (Recommended)

### 1. Clone Repository on Server

```bash
cd /tmp
git clone <Your-Repo-URL> mehngai-monitor-repo
cd mehngai-monitor-repo
```

### 2. Run Setup Script

```bash
sudo bash setup-server.sh
```

When prompted, update the paths in the script if needed.

### 3. Configure Environment Variables

```bash
sudo nano /home/mehngai/mehngai-monitor/.env
```

Add your GEMINI API key:
```
GEMINI_API_KEY=your_key_here
```

Save and exit (Ctrl+O, Enter, Ctrl+X).

### 4. Verify Service

```bash
systemctl status mehngai-monitor
```

---

## Manual Setup (Alternative)

If the script doesn't work, follow these steps:

### 1. Create System User

```bash
sudo useradd -r -s /bin/bash -m -d /home/mehngai mehngai
```

### 2. Setup Application Directory

```bash
sudo mkdir -p /home/mehngai/mehngai-monitor
cd /tmp/mehngai-monitor-repo
sudo cp -r * /home/mehngai/mehngai-monitor/
sudo chown -R mehngai:mehngai /home/mehngai/mehngai-monitor
```

### 3. Setup Python Environment

```bash
cd /home/mehngai/mehngai-monitor
sudo -u mehngai python3 -m venv venv
sudo -u mehngai venv/bin/pip install --upgrade pip
sudo -u mehngai venv/bin/pip install -r requirements.txt
```

### 4. Create .env File

```bash
sudo nano /home/mehngai/mehngai-monitor/.env
```

Add:
```
GEMINI_API_KEY=your_api_key
```

### 5. Install Systemd Service

```bash
sudo cp mehngai-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable mehngai-monitor
sudo systemctl start mehngai-monitor
```

### 6. Verify It's Running

```bash
systemctl status mehngai-monitor
```

---

## Managing the Service

### View Status
```bash
systemctl status mehngai-monitor
```

### View Logs
```bash
# Last 50 lines
journalctl -u mehngai-monitor -n 50

# Follow logs in real-time
journalctl -u mehngai-monitor -f

# Show errors only
journalctl -u mehngai-monitor -p err

# Show last 1 hour
journalctl -u mehngai-monitor --since "1 hour ago"
```

### Control Service
```bash
# Restart
sudo systemctl restart mehngai-monitor

# Stop
sudo systemctl stop mehngai-monitor

# Start
sudo systemctl start mehngai-monitor

# Check startup on boot
sudo systemctl is-enabled mehngai-monitor
```

---

## Accessing the Web Interface

Once running, access the web UI at:
```
http://your-server-ip:8000
```

### Configure Reverse Proxy (Optional but Recommended)

For better production setup, use Nginx as a reverse proxy:

#### Install Nginx
```bash
sudo apt update
sudo apt install -y nginx
```

#### Create Nginx Config
```bash
sudo nano /etc/nginx/sites-available/mehngai-monitor
```

Paste this:
```nginx
server {
    listen 80;
    server_name your-domain.com;  # or your IP
    
    client_max_body_size 50M;
    
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # WebSocket support (if needed)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

#### Enable Config
```bash
sudo ln -s /etc/nginx/sites-available/mehngai-monitor /etc/nginx/sites-enabled/
sudo nginx -t  # Test config
sudo systemctl restart nginx
```

Now access via `http://your-domain.com`

---

## Troubleshooting

### Service Won't Start

Check logs:
```bash
journalctl -u mehngai-monitor -n 100
```

Common issues:
- **Port already in use**: Use `lsof -i :8000` to find the process
- **Missing dependencies**: Re-run `pip install -r requirements.txt`
- **Permission denied**: Check ownership with `ls -la /home/mehngai/mehngai-monitor`

### App Keeps Crashing

Check the logs for errors:
```bash
journalctl -u mehngai-monitor -f
```

### Database Locked Errors

If you see "database is locked", the previous process didn't close properly:
```bash
# Kill any existing Python processes
sudo pkill -f "python.*main:app"

# Restart service
sudo systemctl restart mehngai-monitor
```

### Check If Port 8000 is Accessible

```bash
# From the server
curl http://localhost:8000

# From remote
curl http://your-server-ip:8000
```

---

## Update the Application

To update to the latest version:

```bash
cd /home/mehngai/mehngai-monitor
sudo -u mehngai git pull  # If using git
sudo systemctl restart mehngai-monitor
```

---

## Monitoring & Auto-Restart

The systemd service is configured to automatically:
- **Restart on failure** - up to 5 restart attempts within 60 seconds
- **Start on boot** - service runs on system reboot
- **Capture logs** - all output goes to systemd journal

Check restart statistics:
```bash
systemctl show mehngai-monitor
```

---

## Security Recommendations

1. **Use Firewall**
   ```bash
   sudo ufw allow 80/tcp  # HTTP
   sudo ufw allow 443/tcp # HTTPS (if using SSL)
   sudo ufw enable
   ```

2. **Enable HTTPS** (with nginx + Let's Encrypt)
   ```bash
   sudo apt install certbot python3-certbot-nginx
   sudo certbot --nginx -d your-domain.com
   ```

3. **Lock Down .env**
   ```bash
   sudo chmod 600 /home/mehngai/mehngai-monitor/.env
   ```

4. **Monitor Resources**
   ```bash
   # Check memory usage
   ps aux | grep mehngai
   
   # Check system logs
   dmesg | tail -20
   ```

---

## Need Help?

Check service logs:
```bash
journalctl -u mehngai-monitor --since "2 hours ago"
```

Verify configuration:
```bash
systemctl cat mehngai-monitor
cat /home/mehngai/mehngai-monitor/.env
```
