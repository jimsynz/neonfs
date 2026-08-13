defmodule NeonFS.CSI.NodeResolver do
  @moduledoc """
  Turns the `node_id` a CSI request names into the BEAM node serving it.

  `ControllerPublishVolume` identifies its target with the `node_id` that
  `NodeGetInfo` returned — in a real deployment the Kubernetes node name,
  which has no relationship to a BEAM node name. A claim that must be
  released when *that* node dies has to be held by a process on it, so the
  controller needs the mapping before it can take the claim at all.

  The mapping is a node-mode CSI plugin's own service registration: it
  advertises its `node_id`, and `NeonFS.Client.ServiceInfo` already carries
  the BEAM node it registered from.
  """

  alias NeonFS.CSI.AttachHolder

  @doc """
  The BEAM node whose CSI plugin reports `node_id`.

  An unknown `node_id` is an error rather than a guess: a controller that
  cannot find the node is being asked to attach to somewhere that has left
  the cluster (or has yet to register), and inventing a target would put
  the claim on the wrong node's lifetime.
  """
  @spec beam_node(String.t()) :: {:ok, node()} | {:error, term()}
  def beam_node(node_id) when is_binary(node_id) and node_id != "" do
    :csi
    |> list_services()
    |> Enum.find(fn service ->
      metadata = service.metadata || %{}
      Map.get(metadata, :mode) == :node and Map.get(metadata, :node_id) == node_id
    end)
    |> case do
      %{node: node} -> {:ok, node}
      nil -> {:error, {:unknown_node_id, node_id}}
    end
  end

  def beam_node(_node_id), do: {:error, :node_id_required}

  @doc """
  The holder pid to take a claim with for `node_id` — the pid whose death
  is what releases that node's attachments.
  """
  @spec attach_holder(String.t()) :: {:ok, pid()} | {:error, term()}
  def attach_holder(node_id) do
    with {:ok, node} <- beam_node(node_id) do
      AttachHolder.pid_on(node)
    end
  end

  # A controller co-located with core reads the registry directly, the way
  # `NeonFS.Transport.PoolManager` does — `Discovery` is the interface-node
  # cache and holds nothing on a core node. Configured as an MFA rather
  # than a closure so it survives being set on another node.
  defp list_services(type) do
    case Application.get_env(:neonfs_csi, :service_list_fn) do
      nil -> NeonFS.Client.Discovery.list_by_type(type)
      {module, function, args} -> module |> apply(function, args) |> of_type(type)
      fun when is_function(fun, 1) -> fun.(type)
    end
  end

  defp of_type(services, type) when is_list(services) do
    Enum.filter(services, &(&1.type == type))
  end

  defp of_type(_services, _type), do: []
end
