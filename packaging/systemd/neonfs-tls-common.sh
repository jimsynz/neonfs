#!/bin/bash
# Shared TLS distribution functions for NeonFS daemon wrapper scripts.
#
# Generates a local CA, daemon distribution certificate, and CLI client
# certificate on first boot. These enable TLS-encrypted Erlang distribution
# before the node has joined a cluster.
#
# After cluster join/init, the Elixir code regenerates ca_bundle.crt and
# ssl_dist.conf to include the cluster CA and cluster-signed node certificate.

NEONFS_TLS_DIR="${NEONFS_TLS_DIR:-/var/lib/neonfs/tls}"

# Generate local CA, daemon cert, and CLI cert if not already present.
# Only runs once per node — skips if local-ca.key already exists.
ensure_local_tls() {
    mkdir -p "${NEONFS_TLS_DIR}"

    if [ -f "${NEONFS_TLS_DIR}/local-ca.key" ]; then
        return 0
    fi

    echo "Generating local TLS certificates for distribution..."

    local tmp_dir
    tmp_dir=$(mktemp -d)
    trap "rm -rf ${tmp_dir}" EXIT

    # 1. Local CA (self-signed, CA:true, pathlen:0)
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
        -keyout "${tmp_dir}/local-ca.key" \
        -out "${tmp_dir}/local-ca.crt" \
        -days 3650 -nodes \
        -subj "/O=NeonFS/CN=local CA" \
        -addext "basicConstraints=critical,CA:true,pathlen:0" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" \
        2>/dev/null

    # 2. Daemon distribution certificate (signed by local CA)
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
        -keyout "${tmp_dir}/node-local.key" \
        -out "${tmp_dir}/node-local.csr" \
        -nodes -subj "/O=NeonFS/CN=node-local" \
        2>/dev/null

    openssl x509 -req \
        -in "${tmp_dir}/node-local.csr" \
        -CA "${tmp_dir}/local-ca.crt" \
        -CAkey "${tmp_dir}/local-ca.key" \
        -CAcreateserial \
        -out "${tmp_dir}/node-local.crt" \
        -days 3650 \
        -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1\nextendedKeyUsage=serverAuth,clientAuth") \
        2>/dev/null

    # 3. CLI client certificate (signed by local CA)
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
        -keyout "${tmp_dir}/cli.key" \
        -out "${tmp_dir}/cli.csr" \
        -nodes -subj "/O=NeonFS/CN=cli" \
        2>/dev/null

    openssl x509 -req \
        -in "${tmp_dir}/cli.csr" \
        -CA "${tmp_dir}/local-ca.crt" \
        -CAkey "${tmp_dir}/local-ca.key" \
        -CAcreateserial \
        -out "${tmp_dir}/cli.crt" \
        -days 3650 \
        -extfile <(printf "extendedKeyUsage=clientAuth") \
        2>/dev/null

    # Move certificates into place atomically
    cp "${tmp_dir}/local-ca.crt" "${NEONFS_TLS_DIR}/local-ca.crt"
    cp "${tmp_dir}/local-ca.key" "${NEONFS_TLS_DIR}/local-ca.key"
    cp "${tmp_dir}/node-local.crt" "${NEONFS_TLS_DIR}/node-local.crt"
    cp "${tmp_dir}/node-local.key" "${NEONFS_TLS_DIR}/node-local.key"
    cp "${tmp_dir}/cli.crt" "${NEONFS_TLS_DIR}/cli.crt"
    cp "${tmp_dir}/cli.key" "${NEONFS_TLS_DIR}/cli.key"

    # Set permissions: daemon keys 0600, CLI key 0640, certs 0644
    chmod 0600 "${NEONFS_TLS_DIR}/local-ca.key"
    chmod 0600 "${NEONFS_TLS_DIR}/node-local.key"
    chmod 0640 "${NEONFS_TLS_DIR}/cli.key"
    chmod 0644 "${NEONFS_TLS_DIR}/local-ca.crt"
    chmod 0644 "${NEONFS_TLS_DIR}/node-local.crt"
    chmod 0644 "${NEONFS_TLS_DIR}/cli.crt"

    # Set ownership if running as root
    if [ "$EUID" -eq 0 ]; then
        chown neonfs:neonfs "${NEONFS_TLS_DIR}"/*
    fi

    # Generate initial CA bundle and ssl_dist.conf
    generate_ca_bundle
    generate_ssl_dist_conf

    trap - EXIT
    rm -rf "${tmp_dir}"

    echo "Local TLS certificates generated in ${NEONFS_TLS_DIR}"
}

# Regenerate ca_bundle.crt from available CA certificates.
# Contains local-ca.crt, and also cluster ca.crt if present.
generate_ca_bundle() {
    local bundle="${NEONFS_TLS_DIR}/ca_bundle.crt"

    cp "${NEONFS_TLS_DIR}/local-ca.crt" "${bundle}"

    if [ -f "${NEONFS_TLS_DIR}/ca.crt" ]; then
        cat "${NEONFS_TLS_DIR}/ca.crt" >> "${bundle}"
    fi

    chmod 0644 "${bundle}"
}

# Generate ssl_dist.conf for Erlang TLS distribution.
# Uses node-local cert by default. After cluster join, the Elixir code
# regenerates this to include the cluster node cert via certs_keys.
generate_ssl_dist_conf() {
    local conf="${NEONFS_TLS_DIR}/ssl_dist.conf"
    local tls="${NEONFS_TLS_DIR}"

    cat > "${conf}" <<ERLCONF
[{server, [
  {certs_keys, [#{certfile => "${tls}/node-local.crt",
                  keyfile => "${tls}/node-local.key"}]},
  {cacertfile, "${tls}/ca_bundle.crt"},
  {verify, verify_peer},
  {fail_if_no_peer_cert, true},
  {versions, ['tlsv1.3']}
]},
{client, [
  {certs_keys, [#{certfile => "${tls}/node-local.crt",
                  keyfile => "${tls}/node-local.key"}]},
  {cacertfile, "${tls}/ca_bundle.crt"},
  {verify, verify_peer},
  {versions, ['tlsv1.3']}
]}].
ERLCONF

    chmod 0644 "${conf}"
}
