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

# Resolve the node's distribution hostname (the "host" half of name@host).
# Sources, in order of preference:
#   1. RELEASE_NODE env var (release-set, contains the `name@host` form)
#   2. `hostname -f` for the FQDN
#   3. `hostname` as a fallback
# Prints the resolved hostname on stdout, or empty if none could be resolved.
resolve_dist_hostname() {
    local host=""

    if [ -n "${RELEASE_NODE:-}" ]; then
        host="${RELEASE_NODE#*@}"
    fi

    if [ -z "$host" ]; then
        host=$(hostname -f 2>/dev/null) || host=""
    fi

    if [ -z "$host" ]; then
        host=$(hostname 2>/dev/null) || host=""
    fi

    echo "$host"
}

# Heuristically detect whether a string is an IP address (v4 or v6).
# A colon is sufficient for v6; v4 requires four numeric octets.
is_ip_address() {
    local addr="$1"

    case "$addr" in
        *:*) return 0 ;;
    esac

    case "$addr" in
        *.*.*.*)
            local IFS=.
            # shellcheck disable=SC2086
            set -- $addr
            [ $# -eq 4 ] || return 1
            for octet; do
                case "$octet" in
                    ""|*[!0-9]*) return 1 ;;
                esac
                [ "$octet" -le 255 ] || return 1
            done
            return 0
            ;;
        *) return 1 ;;
    esac
}

# Build the subjectAltName string for the distribution certificate.
# Always includes localhost / 127.0.0.1 / ::1; if a real hostname is
# available, appends it as DNS or IP depending on shape.
build_dist_san() {
    local sans="DNS:localhost,IP:127.0.0.1,IP:::1"
    local hostname
    hostname=$(resolve_dist_hostname)

    if [ -n "$hostname" ] && [ "$hostname" != "localhost" ]; then
        if is_ip_address "$hostname"; then
            sans="${sans},IP:${hostname}"
        else
            sans="${sans},DNS:${hostname}"
        fi
    fi

    echo "$sans"
}

# Sign the daemon distribution certificate with the supplied SAN string.
# Caller must have created the CA, the CSR (node-local.csr), and the key
# already; this only does the signing step so it can be reused both at
# first-boot and during regeneration.
sign_dist_cert() {
    local tmp_dir="$1"
    local san="$2"

    openssl x509 -req \
        -in "${tmp_dir}/node-local.csr" \
        -CA "${tmp_dir}/local-ca.crt" \
        -CAkey "${tmp_dir}/local-ca.key" \
        -CAcreateserial \
        -out "${tmp_dir}/node-local.crt" \
        -days 3650 \
        -extfile <(printf "subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth" "${san}") \
        2>/dev/null
}

# Generate local CA, daemon cert, and CLI cert if not already present.
# Only runs once per node — skips if local-ca.key already exists.
ensure_local_tls() {
    mkdir -p "${NEONFS_TLS_DIR}"

    if [ -f "${NEONFS_TLS_DIR}/local-ca.key" ]; then
        ensure_dist_cert_covers_hostname
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

    sign_dist_cert "${tmp_dir}" "$(build_dist_san)"

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

# Inspect the existing distribution cert and regenerate it if its SAN
# does not cover the current distribution hostname. Triggered on every
# `ensure_local_tls` invocation when the CA already exists, so existing
# deployments are healed in place at the next daemon restart.
#
# Only the `node-local` cert is regenerated; the local CA, CA bundle,
# and CLI cert are untouched. The new cert is signed by the existing
# local CA and is therefore trusted by every component that already
# trusts that CA.
ensure_dist_cert_covers_hostname() {
    local cert="${NEONFS_TLS_DIR}/node-local.crt"
    local ca_crt="${NEONFS_TLS_DIR}/local-ca.crt"
    local ca_key="${NEONFS_TLS_DIR}/local-ca.key"
    local key="${NEONFS_TLS_DIR}/node-local.key"

    [ -f "$cert" ] || return 0
    [ -f "$ca_crt" ] || return 0
    [ -f "$ca_key" ] || return 0
    [ -f "$key" ] || return 0

    local hostname
    hostname=$(resolve_dist_hostname)

    [ -n "$hostname" ] || return 0
    [ "$hostname" = "localhost" ] && return 0

    local san_line
    san_line=$(openssl x509 -in "$cert" -noout -ext subjectAltName 2>/dev/null \
        | grep -v "X509v3 Subject Alternative Name") || san_line=""

    case "$san_line" in
        *"DNS:${hostname}"*|*"IP:${hostname}"*)
            return 0
            ;;
    esac

    echo "Distribution cert SAN does not cover '${hostname}', regenerating..."

    local tmp_dir
    tmp_dir=$(mktemp -d)
    trap "rm -rf ${tmp_dir}" EXIT

    cp "$ca_crt" "${tmp_dir}/local-ca.crt"
    cp "$ca_key" "${tmp_dir}/local-ca.key"
    cp "$key" "${tmp_dir}/node-local.key"

    openssl req -new -key "${tmp_dir}/node-local.key" \
        -out "${tmp_dir}/node-local.csr" \
        -subj "/O=NeonFS/CN=node-local" \
        2>/dev/null

    sign_dist_cert "${tmp_dir}" "$(build_dist_san)"

    cp "${tmp_dir}/node-local.crt" "${NEONFS_TLS_DIR}/node-local.crt"
    chmod 0644 "${NEONFS_TLS_DIR}/node-local.crt"

    if [ "$EUID" -eq 0 ]; then
        chown neonfs:neonfs "${NEONFS_TLS_DIR}/node-local.crt"
    fi

    trap - EXIT
    rm -rf "${tmp_dir}"

    echo "Distribution cert regenerated with hostname '${hostname}' in SAN."
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
