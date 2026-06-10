# neonfs_iam

Identity and access management for NeonFS — the Ash domain that will
host `User`, `Group`, `AccessPolicy`, and `IdentityMapping` resources,
plus the authentication/authorisation surface consumed by
`NeonFS.Core.Authorise` and the protocol bridges (S3, WebDAV, FUSE,
NFS, CIFS).

**Status: scaffold.** The domain exists with no registered resources
yet; storage will go through the cluster's Ra-backed KV
(`NeonFS.Client.KV`) rather than a database. See the
[#135 epic](https://harton.dev/project-neon/neonfs/issues/135) for the
architecture decisions and the sub-issue breakdown (#288, #290, #291,
#292).

IAM is deliberately its own package: identity is an orchestration
layer on top of the filesystem, not part of building it. Like every
interface package it depends on [`neonfs_client`](../neonfs_client/)
only — never on `neonfs_core`.

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
