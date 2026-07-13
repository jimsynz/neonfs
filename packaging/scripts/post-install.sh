#!/bin/bash
# Post-installation script for NeonFS packages
# Reloads systemd and enables/starts the service

set -e

# The FUSE/omnibus daemons run as the non-root `neonfs` user but mount on
# behalf of arbitrary-UID consumers (Docker containers, NFS clients). The
# setuid `fusermount3` helper only honours `allow_other` for non-root
# callers when `user_allow_other` is set in /etc/fuse.conf. The file is
# present only where the fuse3 dependency is installed (fuse, omnibus).
if [ -f /etc/fuse.conf ] && ! grep -qE '^[[:space:]]*user_allow_other' /etc/fuse.conf; then
    echo "user_allow_other" >> /etc/fuse.conf
fi

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload

    for service in neonfs-core neonfs-fuse neonfs-nfs neonfs-s3 neonfs-webdav neonfs-docker neonfs-containerd neonfs-cifs neonfs-omnibus; do
        if [ -f "/usr/lib/systemd/system/${service}.service" ]; then
            systemctl enable "${service}.service" || true
            systemctl start "${service}.service" || true
        fi
    done
fi
