defmodule NeonFS.Integration.TelemetryForwarder do
  @moduledoc """
  Attaches a telemetry handler on a peer node that forwards matching events
  back to a test pid on the test runner.

  Lives in `test/support/` so it compiles into
  `_build/test/lib/neonfs_integration/ebin/` and is therefore available in
  the peer's code path (peers inherit `:code.get_path/0` from the test
  runner). Anonymous-function closures embedded in the test module itself
  cannot be used for cross-node telemetry handlers because the test
  module's beam is not on disk and the peer cannot resolve it.

  ## Usage

      ref = make_ref()

      :ok =
        PeerCluster.rpc(cluster, :node1, TelemetryForwarder, :attach, [
          test_pid,
          ref,
          [:neonfs, :escalation, :pending_by_category]
        ])

      # ... trigger the event on node1 ...

      assert_receive {:telemetry_forwarded, ^ref, _event, _measurements, _metadata}
  """

  alias NeonFS.Core.RaSupervisor

  @doc """
  Attaches a telemetry handler on the current node for `event` that sends
  `{:telemetry_forwarded, ref, event, measurements, metadata}` to
  `target_pid`. Returns `:ok` on success.
  """
  @spec attach(pid(), reference(), [atom()]) :: :ok | {:error, :already_exists}
  def attach(target_pid, ref, event) when is_pid(target_pid) and is_reference(ref) do
    :telemetry.attach(
      handler_id(ref),
      event,
      &__MODULE__.handle_event/4,
      %{target_pid: target_pid, ref: ref}
    )
  end

  @doc """
  Detaches a previously-attached forwarder. Safe to call even if the
  handler has already been detached.
  """
  @spec detach(reference()) :: :ok
  def detach(ref) when is_reference(ref) do
    :telemetry.detach(handler_id(ref))
    :ok
  end

  @doc false
  def handle_event(event, measurements, metadata, %{target_pid: pid, ref: ref}) do
    send(pid, {:telemetry_forwarded, ref, event, measurements, metadata})
    :ok
  end

  @doc """
  Queries the Ra metadata state machine for a single escalation by ID.
  Runs on a peer node (where it's invoked via RPC) so the query closure
  resolves to a module that exists in the peer's code path — the test
  module's beam does not.
  """
  @spec query_escalation(String.t()) :: {:ok, map()} | {:error, term()}
  def query_escalation(id) when is_binary(id) do
    RaSupervisor.query(fn state ->
      state
      |> Map.get(:escalations, %{})
      |> Map.get(id)
    end)
  end

  @doc """
  Test seam for `NeonFS.Core.DRSnapshotScheduler`'s `:create_fn` config.
  Lives on the peer's code path (this module compiles into
  `_build/test/lib/neonfs_integration/ebin/`) so the scheduler can call
  it across nodes without resolving a closure from the test module.

  Returns the same shape as `NeonFS.Core.DRSnapshot.create/1` —
  `{:ok, %{path: String.t()}}` — without writing anything to the
  `_system` volume, so the scheduler's `[:neonfs, :dr_snapshot_scheduler,
  :tick]` telemetry fires once per leader tick at the configured cadence.
  Used by the peer-cluster scheduling test (#570).
  """
  @spec dr_snapshot_create_fn(keyword()) :: {:ok, %{path: String.t()}}
  def dr_snapshot_create_fn(_opts \\ []) do
    ts =
      DateTime.utc_now()
      |> DateTime.to_unix(:microsecond)
      |> Integer.to_string()

    {:ok, %{path: "/dr/test-#{ts}"}}
  end

  defp handler_id(ref), do: {__MODULE__, ref}
end
