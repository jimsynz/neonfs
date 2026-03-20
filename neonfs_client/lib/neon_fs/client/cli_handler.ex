defmodule NeonFS.Client.CLIHandler do
  @moduledoc """
  RPC target for CLI health check commands.

  Present on all node types (core, FUSE, NFS, omnibus) since neonfs_client
  is a universal dependency. The CLI binary calls this module via Erlang
  distribution to get health status regardless of node type.
  """

  require Logger

  alias NeonFS.Client.HealthCheck

  @doc """
  Returns serialised node health status for the CLI.
  """
  @spec handle_node_status() :: {:ok, map()}
  def handle_node_status do
    Logger.metadata(
      component: :cli,
      request_id: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    )

    HealthCheck.handle_node_status()
  end
end
