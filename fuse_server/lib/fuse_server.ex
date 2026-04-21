defmodule FuseServer do
  @moduledoc """
  A standalone Elixir library for building FUSE-based userspace filesystems.

  Today this library exposes only the low-level transport between the BEAM
  and the Linux FUSE kernel ABI (see `FuseServer.Native`). Later sub-issues
  under the native-BEAM FUSE epic will add the protocol codec, INIT
  handshake, opcode dispatch, and a `FuseServer.Backend` behaviour for
  storage callbacks.

  The transport is intentionally small — it owns the `/dev/fuse` file
  descriptor and hands raw request/response frames to the caller. A
  consumer typically waits for a read-readiness notification via
  `FuseServer.Native.select_read/1`, then uses
  `FuseServer.Native.read_frame/1` and `FuseServer.Native.write_frame/2`
  to exchange bounded frames with the kernel.
  """
end
