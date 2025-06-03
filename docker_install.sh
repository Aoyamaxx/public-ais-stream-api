#!/usr/bin/env bash
#
# install_docker_ubuntu.sh
#
# Usage:
#   chmod +x docker_install.sh
#   ./docker_install.sh
#   bash docker_install.sh

set -e

echo ">>> [1/5] Updating APT package index..."
sudo apt-get update -y

echo ">>> [2/5] Installing dependency packages..."
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg

echo ">>> [3/5] Adding Docker official GPG key and apt repository..."
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu \
$(lsb_release -cs) stable" \
| sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

echo ">>> [4/5] Installing Docker Engine..."
sudo apt-get update -y
sudo apt-get install -y \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin

echo ">>> [5/5] Adding current user to the 'docker' group (a re-login is required)..."
sudo usermod -aG docker "$USER"

echo ">>> Docker installation complete. Please log out and log back in or open a new terminal to use Docker without sudo."
