# NeonFS S3

An S3-compatible HTTP front end for NeonFS volumes. Point the `aws`
CLI, an SDK, or any S3-speaking tool at the cluster: buckets map to
NeonFS volumes, object keys map to file paths, and the objects you put
are the same files everyone else sees over FUSE, NFS, and WebDAV.

Supports AWS Signature v4 authentication (access keys resolved against
the cluster), multipart uploads, RFC 7232 conditional requests, and
virtual-hosted-style addressing. Uploads and downloads stream chunk by
chunk — memory use is bounded regardless of object size.

The S3 protocol layer is the [`firkin`](https://hex.pm/packages/firkin)
library; this package supplies the NeonFS backend. It depends on
[`neonfs_client`](../neonfs_client/) only — it has **no dependency on
`neonfs_core`**.

## Architecture

```
aws-cli / SDK ──── HTTP ──────→ neonfs_s3 (Bandit + Firkin.Plug)
                                     │
                                     │ NeonFS.Client.Router (metadata)
                                     │ TLS data plane (chunk bytes)
                                     ▼
                                neonfs_core (volumes, metadata, blob store)
```

## Building and testing

```bash
mix deps.get
mix compile
mix test                          # unit tests (run without core)
mix check --no-retry              # full check suite
```

End-to-end S3 tests (including streaming memory profiles) live in
`test/integration/`.

## Configuration

Environment variables (read at release startup via `config/runtime.exs`):

| Variable                 | Default                 | Description                          |
|--------------------------|-------------------------|--------------------------------------|
| `NEONFS_CORE_NODE`       | `neonfs_core@localhost` | Core node to connect to              |
| `NEONFS_S3_PORT`         | `8080`                  | S3 HTTP listen port                  |
| `NEONFS_S3_BIND`         | `0.0.0.0`               | S3 HTTP bind address                 |
| `NEONFS_S3_REGION`       | `neonfs`                | Region returned by GetBucketLocation |
| `NEONFS_S3_METRICS`      | `false`                 | Enable Prometheus metrics endpoint   |
| `NEONFS_S3_METRICS_PORT` | `9571`                  | Metrics endpoint port                |
| `NEONFS_S3_METRICS_BIND` | `0.0.0.0`               | Metrics endpoint bind address        |

## Running

As an OTP release:

```bash
mix release neonfs_s3
_build/prod/rel/neonfs_s3/bin/neonfs_s3 start
```

Or as a container:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f ../containers/bake.hcl --load s3
```

Credentials are managed with the CLI (`neonfs s3 ...`) — see the
[user guide](../docs/user-guide.md) for client configuration examples.

## Licence

Apache-2.0 — see [LICENSE](../LICENSE) for details.
