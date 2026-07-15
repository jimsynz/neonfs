# neonfs_cifs

CIFS/SMB access to NeonFS — native Windows and macOS file shares
backed by the cluster, via a Samba VFS module. **In progress**: the
Elixir side and the Samba-side `vfs_neonfs.so` C shim are implemented
and CI-tested; packaging and the end-to-end test are still to come
(see the [#116 epic](https://harton.dev/project-neon/neonfs/issues/116)).

## How it will fit together

Samba handles the SMB protocol, authentication, and Windows semantics
— problems already solved well — while a thin VFS module
(`vfs_neonfs.so`) forwards filesystem operations to this package over
a Unix domain socket, and from there through `neonfs_client` to the
cluster. The framing is 4-byte big-endian length-prefixed ETF
(`:erlang.term_to_binary/1` end to end — no JSON, no protobuf).

## What's implemented (this package)

- `NeonFS.CIFS.Application` — owns the listener, per-connection state,
  and the cluster service-registry registration.
- A `ThousandIsland`-based UDS server with length-prefix framing.
- A handler dispatching each `{:op_atom, args}` tuple to a per-VFS-op
  handler, covering the ~20 "must implement" Samba VFS ops (lifecycle,
  metadata, file I/O, directory iteration, mutations, statvfs).
- A per-connection handle table — synthetic 64-bit tokens mapping to
  NeonFS volume + path, so the C shim can present POSIX-style fds.

## What's built alongside this package

- `vfs_neonfs.so` C shim and in-tree pinned-Samba build, CI-tested
  ([#384](https://harton.dev/project-neon/neonfs/issues/384); see
  [`native/vfs_neonfs/`](native/vfs_neonfs/))

## What's outstanding

- End-to-end test against a real `smbd`
  ([#386](https://harton.dev/project-neon/neonfs/issues/386))

Container image ([#1528](https://harton.dev/project-neon/neonfs/issues/1528)),
Debian package ([#1527](https://harton.dev/project-neon/neonfs/issues/1527)),
and omnibus integration ([#1468](https://harton.dev/project-neon/neonfs/issues/1468))
have landed.

## Debian package

The `neonfs-cifs` deb ships the Elixir bridge (a systemd `notify` service,
socket at `/run/neonfs/cifs.sock`) and `depends: samba-vfs-neonfs`, a separate
package carrying the compiled Samba VFS module at
`/usr/lib/<arch>-linux-gnu/samba/vfs/neonfs.so`. A Samba VFS module is ABI- and
symbol-version-locked to the Samba it was built against, so `samba-vfs-neonfs`
is built from the **distro's own Samba source** (`apt-get source samba`, the
same way Debian builds `samba-vfs-ceph`) and `depends: samba (= exact version)`
— guaranteeing it loads in the host `smbd` (#1548, superseding #1527).

After installing the deb, wire an SMB share to a NeonFS volume in
`/etc/samba/smb.conf`:

```ini
[myvolume]
  vfs objects = neonfs
  neonfs:socket = /run/neonfs/cifs.sock
  neonfs:volume = myvolume
  read only = no
```

then `systemctl enable --now neonfs-cifs` and `systemctl restart smbd`. The
same snippet is shipped as a reference in `/etc/neonfs/cifs.conf`.

## Omnibus deployment

The `neonfs-omnibus` deb and container image serve CIFS too (the bridge runs
in-process; see [#1546](https://harton.dev/project-neon/neonfs/issues/1546)).
`neonfs-omnibus` and `neonfs-cifs` are mutually exclusive — install one or the
other, not both.

- **Omnibus deb** — `depends: samba-vfs-neonfs` (the same VFS-module package
  the standalone `neonfs-cifs` deb uses). Set the bridge socket via
  `NEONFS_CIFS_SOCKET` in
  `/etc/neonfs/neonfs.conf` (default `/run/neonfs/cifs.sock`), then wire the
  same `smb.conf` share as above and `systemctl restart smbd`.
- **Omnibus image** — ships the module at `/app/vfs/neonfs.so` for a
  co-located/sidecar `smbd` to load (copy it into the smbd container's VFS
  module dir, or share it via a volume), and exposes the bridge socket under
  the `/run/neonfs` volume.

## Building and testing

```bash
mix deps.get
mix compile
mix test
mix check --no-retry
```

## Licence

Apache-2.0 — see [LICENSE](../LICENSE) for details.
