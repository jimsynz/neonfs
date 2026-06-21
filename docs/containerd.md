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

Once the plugin daemon is running and bound to its UDS, configure
`containerd`'s `config.toml` to disable the built-in content store and
add a `[proxy_plugins]` block:

```toml
# /etc/containerd/config.toml

version = 3
disabled_plugins = ["io.containerd.content.v1.content"]

[proxy_plugins]
  [proxy_plugins.neonfs]
    type    = "content"
    address = "/run/neonfs/containerd.sock"
```

The `disabled_plugins` line is **required** on containerd 2.x and is the
step most likely to be missed. A `type = "content"` proxy registers as
`io.containerd.content.v1.neonfs`, the same plugin type as containerd's
built-in local store `io.containerd.content.v1.content`. containerd 2.0
changed `metadata.v1.bolt` to resolve its content store with
`GetSingle`, which requires *exactly one* plugin of that type; with both
present, metadata fails to load —

```
failed to load plugin error="multiple plugins registered for io.containerd.content.v1: plugin: multiple instances" id=io.containerd.metadata.v1.bolt
```

— and every service that depends on it (`content`, `leases`, `images`,
snapshots, tasks, …) cascades, so the entire gRPC API returns
`unknown service`. (containerd 1.x silently picked the first content
plugin, masking this; the change isn't called out in the 2.0 migration
notes.) Disabling the built-in store leaves NeonFS as the **sole**
content store for that containerd — there is no local fallback. If
`disabled_plugins` already exists in the config (or in an imported
`conf.d/*.toml`), append the entry to that array rather than declaring
the key a second time.

Reload containerd (`systemctl restart containerd`) and verify the
plugin shows up in `containerd config dump`:

```console
$ containerd config dump | grep -A2 'proxy_plugins.neonfs'
[proxy_plugins.neonfs]
  type = "content"
  address = "/run/neonfs/containerd.sock"
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
`/run/neonfs` so containerd on the host can reach the socket):

```console
docker run -d --name neonfs-containerd \
  --network host \
  -v /var/lib/neonfs:/var/lib/neonfs \
  -v /run/neonfs:/run/neonfs \
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
The package depends on `neonfs-common` (TLS material) and
`neonfs-cli`; it conflicts with `neonfs-omnibus` (the all-in-one
package ships the plugin already).

The systemd unit lives at `/usr/lib/systemd/system/neonfs-containerd.service`
and binds its socket inside the daemon's own `RuntimeDirectory`
(`/run/neonfs/containerd.sock`), so the listener doesn't depend on
containerd having been started first.

### Omnibus

`neonfs-omnibus` (single-binary all-in-one) includes the containerd
plugin from this release onwards. No additional config — the
`[proxy_plugins]` block above still points at the same socket path.

## Configuration

Application env (`:neonfs_containerd`):

| Key | Default | Notes |
|-----|---------|-------|
| `:socket_path` | `/run/neonfs/containerd.sock` | Path containerd's `[proxy_plugins]` config dials. The daemon's `RuntimeDirectory=neonfs` makes this writable without privilege escalation; if you override, make sure containerd can read+connect to the new path. |
| `:listener` | `:socket` | `:socket` for production UDS, `{:tcp, port}` for tests / debugging. |
| `:volume` | `"containerd"` | NeonFS volume that holds the content-addressable blobs. Create it once with `neonfs cluster volume create containerd` before pointing containerd at the plugin. |
| `:register_service` | `true` | Register as `:containerd` in the cluster service registry. Disable for transient debugging. |

## Troubleshooting

- **Every command fails with `unknown service containerd.services.<X>`**
  (`leases`, `content`, …) — the built-in content store wasn't disabled,
  so containerd 2.x's metadata plugin hit
  `multiple plugins registered for io.containerd.content.v1` and the
  whole gRPC API failed to register. Add
  `disabled_plugins = ["io.containerd.content.v1.content"]` (see [Wiring
  containerd](#wiring-containerd)) and restart. Confirm in
  `journalctl -u containerd` that `metadata.v1.bolt` loads without a
  `multiple instances` error.
- **`failed to lease content: blob not found`** — containerd's metadata
  DB references blobs that lived in the now-disabled local store but
  aren't in NeonFS. Pull a fresh image (a new digest writes cleanly), or
  on a host with no running containers reset
  `/var/lib/containerd/io.containerd.metadata.v1.bolt` and restart
  containerd.
- **`ctr image pull` hangs** — check the plugin's socket exists and
  containerd can dial it: `ls -la /run/neonfs/containerd.sock` and
  confirm the address in `containerd config dump` matches.
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
