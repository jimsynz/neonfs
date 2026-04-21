defmodule NFSServer do
  @moduledoc """
  A standalone Elixir library for building NFSv3 file servers natively
  on the BEAM.

  Today this library exposes only the XDR codec (see `NFSServer.XDR`).
  Later sub-issues under the native-BEAM NFS epic will add the ONC RPC
  framework (record marking, AUTH_SYS, portmapper), the MOUNT
  protocol, NFSv3 procedures, and a backend behaviour for storage
  callbacks.
  """
end
