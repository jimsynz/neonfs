#!/bin/bash
# Pre-removal script for NeonFS packages
# Stops and disables the service before files are removed

set -e

# Determine which service to stop based on installed units
for service in neonfs-core neonfs-fuse neonfs-nfs neonfs-s3 neonfs-webdav neonfs-omnibus; do
    if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet "${service}.service" 2>/dev/null; then
        systemctl stop "${service}.service" || true
    fi
    if command -v systemctl >/dev/null 2>&1 && systemctl is-enabled --quiet "${service}.service" 2>/dev/null; then
        systemctl disable "${service}.service" || true
    fi
done
