#!/bin/bash
# Validate systemd unit files
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SYSTEMD_DIR="${SCRIPT_DIR}/../systemd"

echo "Validating systemd unit files..."

if ! command -v systemd-analyze &> /dev/null; then
    echo "Warning: systemd-analyze not found, skipping validation"
    exit 0
fi

# Validate the service file
if [ -f "${SYSTEMD_DIR}/neonfs.service" ]; then
    echo "Validating neonfs.service..."
    systemd-analyze verify "${SYSTEMD_DIR}/neonfs.service" || {
        echo "Error: neonfs.service validation failed"
        exit 1
    }
    echo "✓ neonfs.service is valid"
else
    echo "Error: neonfs.service not found"
    exit 1
fi

echo "All systemd unit files are valid"
