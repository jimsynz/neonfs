# API Surfaces

This document describes the external interfaces for accessing NeonFS: S3, FUSE, Docker/Podman, CSI, and CIFS.

## S3-Compatible API

Standard S3 operations mapped to NeonFS:

| S3 Operation | NeonFS Mapping |
|--------------|----------------|
| CreateBucket | Create volume |
| PutObject | Write file |
| GetObject | Read file |
| DeleteObject | Delete file (CoW, GC later) |
| ListObjects | Directory listing |
| HeadObject | File metadata |

Authentication via S3 signature v4 mapped to NeonFS users.

## FUSE Mount

Local filesystem access via FUSE driver:

```bash
$ neonfs mount /mnt/cluster --volume documents
```

Supports standard POSIX operations. Limitations:
- No hard links (content-addressed model)
- No extended attributes (future consideration)
- Append-only semantics for concurrent writers

## Docker/Podman Volume Plugin

HTTP-based plugin protocol:

```
POST /VolumeDriver.Create   → Create subvolume
POST /VolumeDriver.Mount    → FUSE mount at container path
POST /VolumeDriver.Unmount  → Unmount
POST /VolumeDriver.Remove   → Delete subvolume (optional)
```

Usage:

```bash
$ docker volume create -d neonfs -o volume=mydata myvolume
$ docker run -v myvolume:/data myimage
```

## CSI Driver (Kubernetes)

gRPC interface implementing Container Storage Interface:

**Controller Service:**
- CreateVolume / DeleteVolume
- ControllerPublishVolume / ControllerUnpublishVolume

**Node Service:**
- NodeStageVolume / NodeUnstageVolume
- NodePublishVolume / NodeUnpublishVolume

## CIFS/SMB (via Samba)

Samba VFS module translates SMB operations to NeonFS API calls. Enables Windows client access and macOS Finder integration.
