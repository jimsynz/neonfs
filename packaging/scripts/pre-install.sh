#!/bin/bash
# Pre-installation script for NeonFS
# This script creates the neonfs user and necessary directories
# Should be run during package installation (e.g., in .deb/.rpm pre-install hooks)

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Create neonfs system user and group
create_user() {
    if ! getent group neonfs >/dev/null 2>&1; then
        log_info "Creating neonfs group..."
        groupadd --system neonfs
    else
        log_info "Group 'neonfs' already exists"
    fi

    if ! getent passwd neonfs >/dev/null 2>&1; then
        log_info "Creating neonfs user..."
        useradd --system \
            --gid neonfs \
            --home-dir /var/lib/neonfs \
            --no-create-home \
            --shell /usr/sbin/nologin \
            --comment "NeonFS Daemon User" \
            neonfs
    else
        log_info "User 'neonfs' already exists"
    fi
}

# Create necessary directories
create_directories() {
    log_info "Creating directories..."

    # Data directory structure
    mkdir -p /var/lib/neonfs/data/{hot,warm,cold}
    mkdir -p /var/lib/neonfs/meta
    mkdir -p /var/lib/neonfs/wal

    # Runtime directory (will be managed by systemd RuntimeDirectory)
    # But create it here for manual installations
    mkdir -p /run/neonfs

    # Configuration directory
    mkdir -p /etc/neonfs

    log_info "Setting ownership and permissions..."

    # Set ownership
    chown -R neonfs:neonfs /var/lib/neonfs
    chown -R neonfs:neonfs /run/neonfs
    chown -R neonfs:neonfs /etc/neonfs

    # Set permissions
    chmod 750 /var/lib/neonfs
    chmod 750 /var/lib/neonfs/data
    chmod 750 /var/lib/neonfs/meta
    chmod 750 /var/lib/neonfs/wal
    chmod 755 /run/neonfs
    chmod 755 /etc/neonfs
}

# Check for FUSE support
check_fuse() {
    if [ -e /dev/fuse ]; then
        log_info "FUSE support detected"

        # Add neonfs user to fuse group if it exists
        if getent group fuse >/dev/null 2>&1; then
            log_info "Adding neonfs user to fuse group..."
            usermod -a -G fuse neonfs
        fi
    else
        log_warn "FUSE device not found (/dev/fuse)"
        log_warn "Install FUSE support: apt-get install fuse3 (Debian/Ubuntu) or yum install fuse3 (RHEL/CentOS)"
    fi
}

# Install systemd unit files
install_systemd_files() {
    local systemd_dir="/etc/systemd/system"

    if [ -d "$systemd_dir" ]; then
        log_info "Installing systemd unit files..."

        # Copy service file
        if [ -f /tmp/neonfs-install/neonfs.service ]; then
            cp /tmp/neonfs-install/neonfs.service "$systemd_dir/"
            chmod 644 "$systemd_dir/neonfs.service"
        fi

        # Copy environment file
        if [ -f /tmp/neonfs-install/neonfs.conf ]; then
            cp /tmp/neonfs-install/neonfs.conf /etc/neonfs/
            chmod 644 /etc/neonfs/neonfs.conf
        fi

        # Reload systemd
        systemctl daemon-reload
    else
        log_warn "systemd not found, skipping unit file installation"
    fi
}

# Main execution
main() {
    log_info "NeonFS pre-installation starting..."

    # Check if running as root
    if [ "$EUID" -ne 0 ]; then
        echo "Error: This script must be run as root"
        exit 1
    fi

    create_user
    create_directories
    check_fuse

    log_info "Pre-installation complete!"
    log_info "Next steps:"
    log_info "  1. Install the NeonFS package"
    log_info "  2. Enable the service: systemctl enable neonfs"
    log_info "  3. Start the service: systemctl start neonfs"
    log_info "  4. Check status: systemctl status neonfs"
}

main "$@"
