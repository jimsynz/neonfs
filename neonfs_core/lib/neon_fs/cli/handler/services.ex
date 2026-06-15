defmodule NeonFS.CLI.Handler.Services do
  @moduledoc """
  CLI command handler for the cluster service registry.

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `list_services/0` RPC entry point here, so the CLI wire
  contract is unchanged.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.ServiceRegistry

  @doc """
  Lists all registered services in the cluster.

  ## Returns
  - `{:ok, [map]}` - List of service info maps
  """
  @spec list_services() :: {:ok, [map()]}
  def list_services do
    set_cli_metadata()

    with :ok <- require_cluster() do
      services =
        ServiceRegistry.list()
        |> Enum.map(&service_info_to_map/1)

      {:ok, services}
    end
  end

  # Private

  defp service_info_to_map(info) do
    %{
      node: Atom.to_string(info.node),
      type: Atom.to_string(info.type),
      status: Atom.to_string(info.status),
      registered_at: DateTime.to_iso8601(info.registered_at),
      metadata: info.metadata
    }
  end
end
