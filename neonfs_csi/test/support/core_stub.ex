defmodule NeonFS.CSI.TestSupport.CoreStub do
  @moduledoc """
  In-memory, stateful stand-in for the NeonFS core RPCs the CSI
  Controller service issues through `:core_call_fn`.

  The csi-sanity conformance suite drives real create → list →
  snapshot → clone → delete flows and asserts idempotency, so the stub
  must remember what it created across calls. State lives in an `Agent`;
  `handler/1` returns the 3-arity closure the Controller expects.

  Volumes are keyed by name (the CSI `volume_id`); `get_volume/1`
  resolves by name or internal id. Snapshots are keyed by the volume's
  internal id, mirroring `NeonFS.Core.Snapshot`.
  """

  alias NeonFS.Core.Volume

  @capacity 100 * 1024 * 1024 * 1024

  @spec start_link() :: {:ok, pid()}
  def start_link do
    Agent.start_link(fn -> %{volumes: %{}, snaps: %{}, counter: 0} end)
  end

  @spec handler(pid()) :: (module(), atom(), list() -> term())
  def handler(agent) do
    fn module, function, args -> call(agent, module, function, args) end
  end

  defp call(agent, NeonFS.Core, :create_volume, [name, _opts]) do
    Agent.get_and_update(agent, fn state ->
      case Map.fetch(state.volumes, name) do
        {:ok, _existing} ->
          {{:error, %NeonFS.Error.AlreadyExists{}}, state}

        :error ->
          {id, state} = next_id(state, "vol")
          volume = build_volume(id, name)
          {{:ok, volume}, put_in(state.volumes[name], volume)}
      end
    end)
  end

  defp call(agent, NeonFS.Core, :get_volume, [key]) do
    Agent.get(agent, &fetch_volume(&1, key))
  end

  defp call(agent, NeonFS.Core, :delete_volume, [key]) do
    Agent.get_and_update(agent, fn state ->
      case fetch_volume(state, key) do
        {:ok, volume} ->
          {:ok,
           %{
             state
             | volumes: Map.delete(state.volumes, volume.name),
               snaps: Map.delete(state.snaps, volume.id)
           }}

        {:error, :not_found} ->
          {{:error, :not_found}, state}
      end
    end)
  end

  defp call(agent, NeonFS.Core, :list_volumes, []) do
    Agent.get(agent, fn state -> {:ok, Map.values(state.volumes)} end)
  end

  defp call(_agent, NeonFS.Core.StorageMetrics, :cluster_capacity, []) do
    %{total_available: @capacity}
  end

  defp call(agent, NeonFS.Core.Snapshot, :create, [volume_id, opts]) do
    Agent.get_and_update(agent, fn state ->
      {id, state} = next_id(state, "snap")
      snapshot = %{id: id, name: Keyword.get(opts, :name), created_at: DateTime.from_unix!(0)}
      for_volume = state.snaps |> Map.get(volume_id, %{}) |> Map.put(id, snapshot)
      {{:ok, snapshot}, %{state | snaps: Map.put(state.snaps, volume_id, for_volume)}}
    end)
  end

  defp call(agent, NeonFS.Core.Snapshot, :list, [volume_id]) do
    Agent.get(agent, fn state -> {:ok, state.snaps |> Map.get(volume_id, %{}) |> Map.values()} end)
  end

  defp call(agent, NeonFS.Core.Snapshot, :get, [volume_id, snapshot_id]) do
    Agent.get(agent, fn state ->
      case get_in(state.snaps, [volume_id, snapshot_id]) do
        nil -> {:error, :not_found}
        snapshot -> {:ok, snapshot}
      end
    end)
  end

  defp call(agent, NeonFS.Core.Snapshot, :delete, [volume_id, snapshot_id]) do
    Agent.update(agent, fn state ->
      for_volume = state.snaps |> Map.get(volume_id, %{}) |> Map.delete(snapshot_id)
      %{state | snaps: Map.put(state.snaps, volume_id, for_volume)}
    end)
  end

  defp call(agent, NeonFS.Core.Snapshot, :promote, [_source_id, _snapshot_id, new_name, _opts]) do
    case call(agent, NeonFS.Core, :create_volume, [new_name, []]) do
      {:ok, volume} -> {:ok, volume}
      {:error, %NeonFS.Error.AlreadyExists{}} -> call(agent, NeonFS.Core, :get_volume, [new_name])
    end
  end

  defp call(_agent, module, function, args) do
    raise "CoreStub: unhandled core call #{inspect(module)}.#{function}/#{length(args)}"
  end

  defp fetch_volume(state, key) do
    case Map.fetch(state.volumes, key) do
      {:ok, volume} ->
        {:ok, volume}

      :error ->
        case Enum.find(Map.values(state.volumes), &(&1.id == key)) do
          nil -> {:error, :not_found}
          volume -> {:ok, volume}
        end
    end
  end

  defp next_id(state, prefix) do
    counter = state.counter + 1
    {"#{prefix}-#{counter}", %{state | counter: counter}}
  end

  defp build_volume(id, name) do
    %Volume{
      id: id,
      name: name,
      logical_size: 0,
      physical_size: 0,
      chunk_count: 0,
      created_at: DateTime.from_unix!(0),
      updated_at: DateTime.from_unix!(0)
    }
  end
end
