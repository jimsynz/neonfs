defmodule NeonFS.CSI.VolumeHealth do
  @moduledoc """
  CSI v1.3+ `VolumeCondition` computation for the `ControllerGetVolume`
  and `NodeGetVolumeStats` RPCs (sub-issue #316 of the CSI driver epic
  #244).

  The CSI spec exposes a single `VolumeCondition` payload:

  ```protobuf
  message VolumeCondition {
    bool abnormal = 1;
    string message = 2;
  }
  ```

  Two distinct vantage points feed it:

    * **Controller-side** (`controller_condition/2`) — a cluster-wide
      view aggregated from `NeonFS.Core.VolumeRegistry`,
      `NeonFS.Core.StorageMetrics`, `NeonFS.Core.ServiceRegistry`, and
      `NeonFS.Core.Escalation`. Detects insufficient replication
      headroom, no-writable-drives, and pending critical operator
      escalations.
    * **Node-side** (`node_condition/3`) — a host-local probe of the
      FUSE staging path. A wedged mount (`stat(2)` blocking longer than
      the configured timeout) reports `abnormal = true` so kubelet can
      reschedule pods to healthy nodes.

  ## Telemetry

  Each public function compares the result against the previous state
  (kept in `@table`) and emits `[:neonfs, :csi, :volume_condition,
  :transition]` only when the boolean flips. Operators can alert on
  any non-zero count of these events.

    * Measurements: `%{count: 1}`
    * Metadata: `%{volume_id, scope (:controller | :node), to_abnormal,
      message}`

  ## Test injection

    * `:core_call_fn` — `(module, function, args) -> result`. Defaults
      to the `:neonfs_csi` application env entry, falling back to
      `NeonFS.Client.Router.call/3`.
    * `:stat_fn` — `(path) -> {:ok, File.Stat.t()} | {:error, term()}`.
      Defaults to `File.stat/1`.
    * `:stat_timeout_ms` — defaults to `#{2_000}` ms.
  """

  @table :csi_volume_health
  @default_stat_timeout_ms 2_000

  @typedoc "Result returned to the CSI server for direct mapping to `Csi.V1.VolumeCondition`."
  @type condition :: %{abnormal: boolean(), message: String.t()}

  @typedoc "Either `:controller` or `:node` — namespaces transition tracking per vantage point."
  @type scope :: :controller | :node

  ## Lifecycle

  @doc """
  Initialise the per-volume transition-tracking ETS table. Called once
  by the CSI supervisor; idempotent.
  """
  @spec init_table() :: :ok
  def init_table do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    end

    :ok
  end

  @doc "Clears tracked state. Test-only."
  @spec reset_table() :: :ok
  def reset_table do
    init_table()
    :ets.delete_all_objects(@table)
    :ok
  end

  ## Controller-side

  @doc """
  Compute the CSI `VolumeCondition` for `ControllerGetVolume`.

  Aggregates four signals:

    * Replication factor vs. live core node count — under-replicated
      replicate volumes report `abnormal` (degraded semantically).
    * Cluster-wide writable drive count — zero writable drives reports
      `abnormal` (critical).
    * Cluster-wide capacity utilisation — over 95 % reports `abnormal`
      (critical).
    * Pending critical escalations — any open `:critical` escalation
      reports `abnormal` so kubelet doesn't keep scheduling fresh
      pods onto a cluster that needs operator attention.

  Returns `{:error, :not_found}` when the volume itself is gone (the
  CSI server uses this to raise `NOT_FOUND`).
  """
  @spec controller_condition(String.t(), keyword()) ::
          {:ok, condition()} | {:error, :not_found | term()}
  def controller_condition(volume_id, opts \\ []) when is_binary(volume_id) do
    init_table()
    core_call_fn = core_call_fn(opts)

    with {:ok, volume} <- core_call_fn.(NeonFS.Core, :get_volume, [volume_id]) do
      capacity = call_or_default(core_call_fn, NeonFS.Core.StorageMetrics, :cluster_capacity, [])
      core_node_count = count_core_nodes(core_call_fn)

      escalations =
        call_or_default(core_call_fn, NeonFS.Core.Escalation, :list, [[status: :pending]])

      condition = derive_controller_condition(volume, capacity, core_node_count, escalations)
      maybe_emit_transition(volume_id, :controller, condition)
      {:ok, condition}
    end
  end

  ## Node-side

  @doc """
  Compute the CSI `VolumeCondition` for `NodeGetVolumeStats`.

  Probes the local staging path with `File.stat/1` (overridable via
  `:stat_fn`) inside a `Task` bounded by `:stat_timeout_ms`. A timeout
  or non-`:ok` return is treated as a wedged mount and reported as
  `abnormal = true`.
  """
  @spec node_condition(String.t(), Path.t(), keyword()) :: condition()
  def node_condition(volume_id, staging_path, opts \\ [])
      when is_binary(volume_id) and is_binary(staging_path) do
    init_table()
    stat_fn = Keyword.get(opts, :stat_fn, &File.stat/1)
    timeout = Keyword.get(opts, :stat_timeout_ms, @default_stat_timeout_ms)

    condition =
      case probe_with_timeout(stat_fn, staging_path, timeout) do
        :ok ->
          ok_condition()

        {:error, :timeout} ->
          %{abnormal: true, message: "FUSE mount probe at #{staging_path} timed out"}

        {:error, reason} ->
          %{
            abnormal: true,
            message: "FUSE mount probe at #{staging_path} failed: #{inspect(reason)}"
          }
      end

    maybe_emit_transition(volume_id, :node, condition)
    condition
  end

  ## Internals — controller condition

  defp derive_controller_condition(volume, capacity, core_node_count, escalations) do
    cond do
      under_replicated?(volume, core_node_count) ->
        %{
          abnormal: true,
          message:
            "replication factor #{volume.durability.factor} exceeds " <>
              "#{core_node_count} active core node(s)"
        }

      no_writable_drives?(capacity) ->
        %{abnormal: true, message: "no writable drives available in cluster"}

      utilisation_critical?(capacity) ->
        %{abnormal: true, message: "cluster storage utilisation above 95%"}

      Enum.any?(escalations, &critical_escalation?/1) ->
        %{abnormal: true, message: "cluster has pending critical escalations"}

      true ->
        ok_condition()
    end
  end

  defp under_replicated?(%{durability: %{type: :replicate, factor: factor}}, core_node_count)
       when is_integer(core_node_count) and core_node_count > 0 do
    factor > core_node_count
  end

  defp under_replicated?(_, _), do: false

  defp no_writable_drives?(%{drives: drives}) when is_list(drives) do
    drives == [] or Enum.all?(drives, &(&1.state == :draining))
  end

  defp no_writable_drives?(_), do: false

  defp utilisation_critical?(%{total_capacity: cap, total_used: used})
       when is_integer(cap) and cap > 0 and is_integer(used) do
    used / cap > 0.95
  end

  defp utilisation_critical?(_), do: false

  defp critical_escalation?(%{severity: :critical}), do: true
  defp critical_escalation?(_), do: false

  defp ok_condition, do: %{abnormal: false, message: ""}

  ## Internals — node probe

  defp probe_with_timeout(stat_fn, path, timeout) do
    task = Task.async(fn -> stat_fn.(path) end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:ok, _stat}} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      nil -> {:error, :timeout}
      {:exit, reason} -> {:error, reason}
    end
  end

  ## Internals — core call dispatch + transition tracking

  defp call_or_default(core_call_fn, module, function, args) do
    case safe_call(core_call_fn, module, function, args) do
      nil -> []
      {:ok, value} -> value
      value -> value
    end
  end

  defp count_core_nodes(core_call_fn) do
    case safe_call(core_call_fn, NeonFS.Core.ServiceRegistry, :list_by_type, [:core]) do
      list when is_list(list) -> max(length(list), 1)
      _ -> 1
    end
  end

  defp safe_call(core_call_fn, module, function, args) do
    core_call_fn.(module, function, args)
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp maybe_emit_transition(volume_id, scope, %{abnormal: now} = condition) do
    key = {volume_id, scope}

    case :ets.lookup(@table, key) do
      [{^key, ^now}] ->
        :ok

      _ ->
        :ets.insert(@table, {key, now})

        :telemetry.execute(
          [:neonfs, :csi, :volume_condition, :transition],
          %{count: 1},
          %{volume_id: volume_id, scope: scope, to_abnormal: now, message: condition.message}
        )

        :ok
    end
  end

  defp core_call_fn(opts) do
    cond do
      fun = Keyword.get(opts, :core_call_fn) -> fun
      fun = Application.get_env(:neonfs_csi, :core_call_fn) -> fun
      true -> &default_core_call/3
    end
  end

  defp default_core_call(module, function, args) do
    NeonFS.Client.Router.call(module, function, args)
  end
end
