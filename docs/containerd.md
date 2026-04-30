# containerd content store plugin

NeonFS ships a `containerd.services.content.v1.Content` proxy plugin
under `neonfs_containerd` so a `containerd` daemon — in production or
under BuildKit — can use a NeonFS volume as its content-addressable
store. Image layer pulls / pushes flow through the plugin's gRPC
socket, the layers land in NeonFS as sharded `sha256/<ab>/<cd>/<rest>`
blobs (per the design call in [#547][547]), and labels round-trip via
POSIX xattrs prefixed with `containerd.io/`.

This page is the operator-side wiring guide. Architecture and the
full RPC reference live in the [package
README](../neonfs_containerd/README.md).

## Wiring containerd

Once the plugin daemon is running and bound to its UDS, append a
`[proxy_plugins]` block to `containerd`'s `config.toml`:

```toml
# /etc/containerd/config.toml

[proxy_plugins]
  [proxy_plugins.neonfs]
    type    = "content"
    address = "/run/containerd/proxy-plugins/neonfs.sock"
```

Reload containerd (`systemctl restart containerd`) and verify the
plugin shows up in `containerd config dump`:

```console
$ containerd config dump | grep -A2 'proxy_plugins.neonfs'
[proxy_plugins.neonfs]
  type = "content"
  address = "/run/containerd/proxy-plugins/neonfs.sock"
```

The first `ctr image pull` after restart routes through NeonFS — you
can verify with `ctr content active` (which talks the same gRPC
service) or by watching `journalctl -u neonfs-containerd` for the
incoming `Write` / `Read` traces.

## Deploying

### Container image

```console
docker pull ghcr.io/jimsynz/neonfs/containerd:latest
```

The image includes the systemd-aware release plus the `neonfs` CLI.
Run it alongside the core daemon (host networking + bind-mount of
`/run/containerd/proxy-plugins`):

```console
docker run -d --name neonfs-containerd \
  --network host \
  -v /var/lib/neonfs:/var/lib/neonfs \
  -v /run/containerd/proxy-plugins:/run/containerd/proxy-plugins \
  -e NEONFS_CORE_NODE=neonfs_core@core-host \
  ghcr.io/jimsynz/neonfs/containerd:latest
```

Build a single-arch image locally:

```console
PLATFORMS='linux/amd64' docker buildx bake \
  -f containers/bake.hcl --load containerd
```

### Debian package

`apt install neonfs-containerd` after enabling the NeonFS apt repo.
The package depends on `neonfs-common` (cookie / TLS material) and
`neonfs-cli`; it conflicts with `neonfs-omnibus` (the all-in-one
package ships the plugin already).

The systemd unit lives at `/usr/lib/systemd/system/neonfs-containerd.service`
and orders itself after `containerd.service` so the
`/run/containerd/proxy-plugins` directory exists by the time the
plugin tries to bind.

### Omnibus

`neonfs-omnibus` (single-binary all-in-one) includes the containerd
plugin from this release onwards. No additional config — the
`[proxy_plugins]` block above still points at the same socket path.

## Configuration

Application env (`:neonfs_containerd`):

| Key | Default | Notes |
|-----|---------|-------|
| `:socket_path` | `/run/containerd/proxy-plugins/neonfs.sock` | Path containerd's `[proxy_plugins]` config dials. Override per-host if your containerd config writes to a different proxy-plugins root. |
| `:listener` | `:socket` | `:socket` for production UDS, `{:tcp, port}` for tests / debugging. |
| `:volume` | `"containerd"` | NeonFS volume that holds the content-addressable blobs. Create it once with `neonfs cluster volume create containerd` before pointing containerd at the plugin. |
| `:register_service` | `true` | Register as `:containerd` in the cluster service registry. Disable for transient debugging. |

## Troubleshooting

- **`ctr image pull` hangs** — check the plugin can read / write its
  socket directory: `ls -la /run/containerd/proxy-plugins/`. The
  systemd unit grants `ReadWritePaths` to that path; if you've
  customised, add it back.
- **`ctr content active` returns nothing during a slow pull** —
  containerd emits writes per layer with a stable `ref`. The
  in-progress write tracker is the `WriteSession` per ref; check
  `journalctl -u neonfs-containerd | grep ref=` to see what
  containerd actually sent.
- **Stale partial uploads** — `WriteSession.abort_stale/1` aborts
  any session whose `updated_at` is older than 24 h by default. The
  active sweeper-GenServer is a follow-up; for now an operator can
  call it from a remsh: `:rpc.call(node, NeonFS.Containerd.WriteSession,
  :abort_stale, [])`.

[547]: https://harton.dev/project-neon/neonfs/issues/547
