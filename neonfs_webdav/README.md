# NeonFS WebDAV

WebDAV interface for NeonFS volumes.

Maps WebDAV collections to NeonFS volumes and resources to file paths,
communicating with core nodes via `NeonFS.Client.Router`. Follows the
same interface node pattern as `neonfs_nfs` and `neonfs_s3`.

## Architecture

```
Finder / Explorer / rclone ──→ neonfs_webdav (Bandit + WebdavServer.Plug)
                                      │
                                      │ NeonFS.Client.core_call/3
                                      │ (Erlang distribution)
                                      ▼
                               neonfs_core (volumes, metadata, blob store)
```

## Configuration

Environment variables for production deployment:

- `NEONFS_WEBDAV_PORT` — HTTP listen port (default: 8081)
- `NEONFS_WEBDAV_BIND` — Bind address (default: 0.0.0.0)
- `NEONFS_CORE_NODE` — Core node name (default: neonfs_core@localhost)
- `RELEASE_COOKIE` — Erlang distribution cookie (must match cluster)
