#!/bin/bash
# Post-installation script for NeonFS packages
# Reloads systemd and enables the service

set -e

# Reload systemd to pick up new/changed unit files
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload
fi
