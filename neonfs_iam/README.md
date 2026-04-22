# neonfs_iam

Identity and access management for NeonFS.

Hosts the Ash resources (`User`, `Group`, `AccessPolicy`, `IdentityMapping`) and
the public authentication/authorisation surface consumed by `NeonFS.Core.Authorise`
and the protocol bridges (S3, WebDAV, FUSE, NFS).

See [issue #135](https://harton.dev/project-neon/neonfs/issues/135) for the
epic. The package starts out with no registered resources — individual
resources land in follow-up slices.
