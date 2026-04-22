# NFSServer

A standalone Elixir library for building NFSv3 file servers natively
on the BEAM — no `nfs3_server` NIF, no out-of-tree Rust. Follows the
same split-of-concerns pattern as `davy` and `s3_server`: a
protocol-level library here, with the NeonFS-specific backend living
in `neonfs_nfs`.

Today (issue #281) it exposes only the XDR codec — the wire format
for every ONC RPC and NFSv3 data structure. The ONC RPC framework
(#282), MOUNT protocol (#283), NFSv3 procedures (#284, #285), and
cutover from the Rust `nfs3_server` NIF (#286) will all land in this
same package.

See `NFSServer.XDR` for the codec API.
