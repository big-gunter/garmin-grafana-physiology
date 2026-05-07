#!/bin/bash
# =============================================================================
# 01_server_setup.sh
# Initial server hardening and dependency installation
# Run as root on a fresh Ubuntu 22.04 instance
# Tested on Hetzner CAX21 ARM (Ubuntu 22.04)
# =============================================================================
set -euo pipefail

echo "==> Updating system packages..."
apt update && apt upgrade -y

echo "==> Installing Docker..."
curl -fsSL https://get.docker.com | sh

echo "==> Installing dependencies..."
apt install -y \
    docker-compose-plugin \
    git \
    nano \
    htop \
    ufw \
    fail2ban

echo "==> Configuring UFW firewall..."
ufw default deny incoming
ufw default allow outgoing
ufw allow 22444/tcp
ufw --force enable
ufw status verbose

echo "==> Configuring SSH on port 22444..."
sed -i 's/#Port 22/Port 22444/' /etc/ssh/sshd_config
systemctl restart sshd
echo "    WARNING: SSH is now on port 22444"
echo "    Test in a NEW terminal: ssh -p 22444 root@<IP>"
echo "    before closing this session!"

echo "==> Configuring Fail2ban for SSH on port 22444..."
cat > /etc/fail2ban/jail.local << 'EOF'
[DEFAULT]
bantime  = 3600
findtime = 600
maxretry = 5

[sshd]
enabled  = true
port     = 22444
logpath  = %(sshd_log)s
backend  = %(sshd_backend)s
EOF

systemctl enable fail2ban
systemctl restart fail2ban
sleep 3
fail2ban-client status sshd

echo "==> Applying kernel network hardening..."
cat >> /etc/sysctl.conf << 'EOF'

# Network hardening - physiology stack
net.ipv4.tcp_syncookies = 1
net.ipv4.conf.all.rp_filter = 1
net.ipv4.conf.default.rp_filter = 1
net.ipv4.icmp_echo_ignore_broadcasts = 1
net.ipv4.conf.all.accept_redirects = 0
net.ipv6.conf.all.accept_redirects = 0
net.ipv4.conf.all.send_redirects = 0
net.ipv4.conf.all.accept_source_route = 0
EOF
sysctl -p

echo "==> Configuring automatic security updates..."
cat > /etc/apt/apt.conf.d/50unattended-upgrades-local << 'EOF'
Unattended-Upgrade::Automatic-Reboot "true";
Unattended-Upgrade::Automatic-Reboot-Time "03:00";
EOF

echo ""
echo "==> Server setup complete."
echo "    Next step: run 02_folders.sh"
