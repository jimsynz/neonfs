#!/bin/bash
# Post-removal script for NeonFS packages
# Cleans up on purge, reloads systemd on remove

set -e

# Reload systemd after unit files are removed
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload
fi

# On purge, remove user, group, and data
if [ "$1" = "purge" ]; then
    # Remove runtime directory
    rm -rf /run/neonfs

    # Remove config directory (only on purge, not remove)
    rm -rf /etc/neonfs

    # Remove user and group
    if getent passwd neonfs >/dev/null 2>&1; then
        userdel neonfs 2>/dev/null || true
    fi
    if getent group neonfs >/dev/null 2>&1; then
        groupdel neonfs 2>/dev/null || true
    fi

    # Note: /var/lib/neonfs is intentionally NOT removed on purge
    # to prevent accidental data loss. Remove manually if desired.
fi
