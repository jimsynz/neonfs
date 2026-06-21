# NeonFS.Containerd

A containerd content-store plugin for NeonFS. Configure containerd with
NeonFS as a proxy content store and image layers are stored in the
cluster — deduplicated (layers shared between images are stored once),
replicated, and visible to every node — instead of in each host's local
boltdb store.

Speaks the `containerd.services.content.v1.Content` gRPC protocol over
a Unix domain socket. Every content-store RPC is implemented:
`Read` and `Write` (both streaming), `Info`, `List`, `Update`,
`Delete`, `Status`, `ListStatuses`, and `Abort`, plus gRPC `Health.Check`
(reports `SERVING` when the cluster has quorum). Blob reads and writes
stream chunk by chunk — memory use is bounded regardless of layer size.

Depends on [`neonfs_client`](../neonfs_client/) only — it has **no
dependency on `neonfs_core`**.

## Wiring containerd

Point containerd at the plugin socket via `/etc/containerd/config.toml`:

```toml
version = 3
disabled_plugins = ["io.containerd.content.v1.content"]

[proxy_plugins]
  [proxy_plugins.neonfs]
    type = "content"
    address = "/run/neonfs/containerd.sock"
```

The `disabled_plugins` line is **required** on containerd 2.x. A content
proxy registers under the same plugin type as containerd's built-in local
store, and `metadata.v1.bolt` resolves the content store with
`GetSingle` — it accepts exactly one plugin of type
`io.containerd.content.v1`. Leaving the built-in store enabled gives it
two, so metadata (and every service that depends on it — `content`,
`leases`, `images`, …) fails to load with
`multiple plugins registered for io.containerd.content.v1: plugin: multiple instances`,
and the whole containerd gRPC API returns `unknown service`. Disabling
the built-in store makes NeonFS the **sole** content store for that
containerd — there is no local fallback. `disabled_plugins` already
exists in most configs; add the entry to the existing array rather than
declaring the key twice.

Verify with:

```bash
ctr content ingest < layer.tar     # store a blob
ctr content get sha256:...         # read it back
```

> **Switching an existing containerd.** Disabling the built-in store
> orphans any content already pulled into it: containerd's metadata DB
> still references those blobs, but NeonFS doesn't have them, so pulls
> fail with `failed to lease content: blob not found`. Re-pull a fresh
> image (a new digest writes cleanly into NeonFS), or reset
> `/var/lib/containerd/io.containerd.metadata.v1.bolt` on a host with no
> running containers.

## Configuration

Application env (`:neonfs_containerd`):

| Key | Default | Notes |
|-----|---------|-------|
| `:start_supervisor` | `true` | Disable for tests that spin up an isolated listener. |
| `:listener` | `:socket` | `:socket` for production UDS, `{:tcp, port}` for tests. |
| `:socket_path` | `/run/neonfs/containerd.sock` | Path containerd's `[proxy_plugins]` config dials. |
| `:register_service` | `true` | Register as `:containerd` in the cluster service registry. |

## Building and testing

```bash
mix deps.get
mix compile
mix test                          # unit tests (run without core)
mix check --no-retry              # full check suite
```

The [QEMU test rig](../test-rig/)'s acceptance suite drives a real
containerd through `ctr content ingest`/`get` against the plugin.

See [`docs/containerd.md`](../docs/containerd.md) for deployment
details.

## Licence

Apache-2.0 — see [LICENSE](../LICENSE) for details.
