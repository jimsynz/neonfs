defmodule NeonFS.CSI.AttachRegistry do
  @moduledoc """
  Cluster-side record of which node a block volume is attached to.

  The record *is* an exclusive `NeonFS.Core.NamespaceCoordinator` claim on
  the volume's attach path. That is what makes the exclusion real rather
  than advisory: the claim is Ra-backed, so every controller in the cluster
  is refused by the same state, and it is held by a pid on the attached
  node, so that node dying releases it with no one having to notice.

  A local table remembers the claim id, because releasing a claim needs the
  id rather than the path. The table is a convenience and never the
  authority — a controller that restarts loses it, and the coordinator
  still refuses a second attachment exactly as before.
  """

  alias NeonFS.Client.Router
  alias NeonFS.Core.NamespaceCoordinator
  alias NeonFS.CSI.NodeResolver

  @table :neonfs_csi_attachments

  @doc """
  Claims `volume_id` for `node_id`, exclusively.

  Re-attaching to the node that already holds it succeeds — the CO retries,
  and a retry that fails on its own previous success is a stuck volume.

  Returns the publish context handed back to the CO, naming the node the
  attachment was granted to.
  """
  @spec claim(String.t(), String.t()) :: {:ok, %{String.t() => String.t()}} | {:error, term()}
  def claim(volume_id, node_id) do
    init_table()

    case current_holder(volume_id) do
      {:ok, ^node_id} -> {:ok, publish_context(node_id)}
      {:ok, other} -> {:error, {:attached_elsewhere, other}}
      :none -> take_claim(volume_id, node_id)
    end
  end

  @doc """
  Releases `volume_id`'s attachment.

  Idempotent in both the ways a CO can get it wrong: releasing an
  attachment that was never taken, and releasing one whose holder has
  already died and dropped it. A detach that can fail is a volume no
  kubelet can move on from.
  """
  @spec release(String.t(), String.t()) :: :ok
  def release(volume_id, _node_id) do
    init_table()

    case :ets.lookup(@table, volume_id) do
      [{^volume_id, %{claim_id: claim_id}}] ->
        _ = coordinator_call(:release, [claim_id])
        :ets.delete(@table, volume_id)

      [] ->
        :ok
    end

    :ok
  end

  @doc """
  The node this controller knows `volume_id` to be attached to.
  """
  @spec current_holder(String.t()) :: {:ok, String.t()} | :none
  def current_holder(volume_id) do
    init_table()

    case :ets.lookup(@table, volume_id) do
      [{^volume_id, %{node_id: node_id}}] -> {:ok, node_id}
      [] -> :none
    end
  end

  @doc false
  @spec init_table() :: :ok
  def init_table do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
        :ok

      _ref ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  @doc false
  @spec reset() :: :ok
  def reset do
    init_table()
    :ets.delete_all_objects(@table)
    :ok
  end

  defp take_claim(volume_id, node_id) do
    with {:ok, holder} <- NodeResolver.attach_holder(node_id),
         {:ok, claim_id} <-
           coordinator_call(:claim_path_for, [attach_path(volume_id), :exclusive, holder]) do
      :ets.insert(@table, {volume_id, %{claim_id: claim_id, node_id: node_id}})
      {:ok, publish_context(node_id)}
    else
      # The coordinator refusing the claim is the authoritative answer, and
      # it is the one that holds when this controller has no local record —
      # after a restart, or when another controller took the attachment.
      {:error, %NeonFS.Error.Conflict{}} -> {:error, {:attached_elsewhere, "another node"}}
      {:error, _reason} = error -> error
    end
  end

  # One path per volume, so a second node's exclusive claim collides with
  # the first rather than sitting beside it.
  defp attach_path(volume_id), do: "csi:attach:#{volume_id}"

  defp publish_context(node_id), do: %{"neonfs.attached_node" => node_id}

  defp coordinator_call(function, args) do
    case Application.get_env(:neonfs_csi, :coordinator_call_fn) do
      nil -> Router.call(NamespaceCoordinator, function, args)
      fun when is_function(fun, 2) -> fun.(function, args)
    end
  end
end
