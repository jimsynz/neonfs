defmodule NeonFS.Containerd do
  @moduledoc """
  containerd content store plugin for NeonFS.

  See `NeonFS.Containerd.Application` for the OTP boot sequence and
  the package README for an RPC-by-RPC status table. The proto
  bindings live under `Containerd.Services.Content.V1.*` and
  `Grpc.Health.V1.*` (vendored from upstream containerd / grpc-proto
  respectively).
  """

  @doc """
  Returns the version string of the running plugin.
  """
  @spec version() :: String.t()
  def version do
    to_string(Application.spec(:neonfs_containerd, :vsn) || "0.0.0")
  end
end
