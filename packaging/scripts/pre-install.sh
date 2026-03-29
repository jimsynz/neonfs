#!/bin/bash
# Pre-installation script for NeonFS packages
# Creates the neonfs user and necessary directories

set -e

# Create neonfs system user and group
if ! getent group neonfs >/dev/null 2>&1; then
    groupadd --system neonfs
fi

if ! getent passwd neonfs >/dev/null 2>&1; then
    useradd --system \
        --gid neonfs \
        --home-dir /var/lib/neonfs \
        --no-create-home \
        --shell /usr/sbin/nologin \
        --comment "NeonFS Daemon User" \
        neonfs
fi

# Create base directories
mkdir -p /var/lib/neonfs/data
mkdir -p /var/lib/neonfs/meta
mkdir -p /var/lib/neonfs/wal
mkdir -p /run/neonfs
mkdir -p /etc/neonfs

# Set ownership and permissions
chown -R neonfs:neonfs /var/lib/neonfs
chown -R neonfs:neonfs /run/neonfs
chmod 770 /var/lib/neonfs
chmod 755 /run/neonfs
chmod 755 /etc/neonfs

# Add neonfs user to fuse group if it exists (for FUSE-based packages)
if getent group fuse >/dev/null 2>&1; then
    usermod -a -G fuse neonfs 2>/dev/null || true
fi
