# NeonFS S3

S3-compatible API interface for NeonFS volumes.

Maps S3 buckets to NeonFS volumes and S3 object keys to file paths,
communicating with core nodes via `NeonFS.Client.Router`.

## Architecture

```
aws-cli / SDK ──── HTTP ──────→ neonfs_s3 (Bandit + Firkin.Plug)
                                     │
                                     │ NeonFS.Client.Router
                                     │ (Erlang distribution)
                                     ▼
                                neonfs_core (volumes, metadata, blob store)
```

## Configuration

Environment variables for production:

| Variable | Default | Description |
|---|---|---|
| `NEONFS_CORE_NODE` | `neonfs_core@localhost` | Core node to connect to |
| `NEONFS_S3_PORT` | `8080` | S3 HTTP listen port |
| `NEONFS_S3_BIND` | `0.0.0.0` | S3 HTTP bind address |
| `NEONFS_S3_REGION` | `neonfs` | Region returned by GetBucketLocation |
