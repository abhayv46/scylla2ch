#!/bin/bash
# scylla2ch production deploy on i-0b47864b1ca99716f (172.31.252.162)
# Run via SSM: aws ssm start-session --target i-0b47864b1ca99716f --region ap-south-1
set -euo pipefail

echo "=== scylla2ch prod deploy ==="

# 1. Install Docker
if ! command -v docker &>/dev/null; then
    echo ">>> Installing Docker..."
    sudo apt-get update -qq
    sudo apt-get install -y -qq docker.io docker-compose-plugin git
    sudo systemctl enable docker
    sudo systemctl start docker
    sudo usermod -aG docker ssm-user 2>/dev/null || sudo usermod -aG docker ubuntu
    echo ">>> Docker installed. You may need to reconnect for group to take effect."
    echo ">>> Run: newgrp docker"
fi

# 2. Clone repo
if [ ! -d ~/scylla2ch ]; then
    echo ">>> Cloning repo..."
    cd ~
    git clone --depth 1 --branch akv/feat/scylla2ch https://github.com/interact-gp/dating-app-backend.git scylla2ch-repo
    ln -sf ~/scylla2ch-repo/scylla2ch ~/scylla2ch
fi

cd ~/scylla2ch

# 3. Create .env.prod
echo ">>> Creating .env.prod..."
cat > .env.prod << 'ENVEOF'
# ScyllaDB Cloud — PROD chat cluster
SCYLLA_HOST_0=node-0-0.gce-asia-south1.c53c5977d99af85a3f29.clusters.scylla.cloud
SCYLLA_HOST_1=node-0-1.gce-asia-south1.c53c5977d99af85a3f29.clusters.scylla.cloud
SCYLLA_HOST_2=node-0-2.gce-asia-south1.c53c5977d99af85a3f29.clusters.scylla.cloud
SCYLLA_PORT=9142
SCYLLA_KEYSPACE=chat
SCYLLA_USER=scylla
SCYLLA_PASSWORD=FILL_ME
SCYLLA_DC=GCE_ASIA_SOUTH_1
SCYLLA_CA_CERT_BASE64=FILL_ME

# ClickHouse Cloud
CH_HOST=FILL_ME.clickhouse.cloud
CH_PORT=9440
CH_DATABASE=chat
CH_USER=default
CH_PASSWORD=FILL_ME
ENVEOF

echo ""
echo "============================================"
echo "  EDIT .env.prod WITH REAL CREDENTIALS"
echo "  nano .env.prod"
echo "============================================"
echo ""
echo "Then run:"
echo "  sudo docker compose up -d --build"
echo "  sudo docker compose logs -f --tail=50"
