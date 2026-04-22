#!/bin/bash
# Post-installation script for NeonFS packages
# Reloads systemd and enables/starts the service

set -e

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload

    for service in neonfs-core neonfs-fuse neonfs-nfs neonfs-s3 neonfs-webdav neonfs-docker neonfs-omnibus; do
        if [ -f "/usr/lib/systemd/system/${service}.service" ]; then
            systemctl enable "${service}.service" || true
            systemctl start "${service}.service" || true
        fi
    done
fi
