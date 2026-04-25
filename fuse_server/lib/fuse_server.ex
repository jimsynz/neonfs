defmodule FuseServer do
  @moduledoc """
  A standalone Elixir library for building FUSE-based userspace filesystems.

  Today this library exposes the low-level transport between the BEAM
  and the Linux FUSE kernel ABI:

    * `FuseServer.Native` — open `/dev/fuse`, `enif_select`-based
      readiness notifications, and bounded `read_frame/1` /
      `write_frame/2`.
    * `FuseServer.Fusermount` — mount and unmount FUSE filesystems via
      the setuid `fusermount3` helper.

  Later sub-issues under the native-BEAM FUSE epic will add the protocol
  codec, INIT handshake, opcode dispatch, and a `FuseServer.Backend`
  behaviour for storage callbacks.

  ## Typical flow

      {:ok, handle} = FuseServer.Fusermount.mount("/tmp/my-mount")
      :ok = FuseServer.Native.select_read(handle)

      receive do
        {:select, ^handle, :undefined, :ready_input} ->
          {:ok, request_bytes} = FuseServer.Native.read_frame(handle)
          # ... produce `response_bytes` ...
          :ok = FuseServer.Native.write_frame(handle, response_bytes)
      end

      :ok = FuseServer.Fusermount.unmount("/tmp/my-mount")
  """
end
