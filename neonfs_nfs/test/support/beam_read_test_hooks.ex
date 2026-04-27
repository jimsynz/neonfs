defmodule NeonFS.NFS.IntegrationTest.BeamReadTestHooks do
  @moduledoc """
  App-env hook implementations for the BEAM NFSv3 read-path smoke
  test (sub-issue #587 of #533). The test runs the BEAM stack on a
  *core* peer; `core_call_fn` and `read_file_stream_fn` are
  short-circuited to local apply / `NeonFS.Core.read_file_stream/3`
  rather than going through `NeonFS.Client.Router` and
  `NeonFS.Client.ChunkReader` (which are the non-core-peer
  abstractions; the cross-node-via-ChunkReader path is the explicit
  subject of #588).

  Lives in `test/support` so it compiles into `_build/test/lib/.../ebin`
  on the test runner; peer nodes inherit the same code paths via
  `:code.get_path/0`, so `&Mod.local_read_file_stream/4` is a stable
  remote-function capture.
  """

  @spec local_read_file_stream(String.t(), String.t(), non_neg_integer(), non_neg_integer()) ::
          Enumerable.t()
  def local_read_file_stream(volume_name, path, offset, count) do
    # `Core.read_file_stream/3` wraps the stream in `{:ok, %{stream:
    # ..., file_size: ...}}`. The handler's `take_bytes/2` wants the
    # raw `Enumerable.t()`, so unwrap.
    case NeonFS.Core.read_file_stream(volume_name, path, offset: offset, length: count) do
      {:ok, %{stream: stream}} -> stream
      {:error, _} = err -> err
    end
  end

  ## Cross-node-read telemetry collection (#588)
  #
  # The cross-node read smoke test attaches a handler on the
  # interface peer that records every conn-pool checkout (one per
  # data-plane chunk fetch). Counts are stored in a public ETS table
  # so the test can read them back via an RPC and assert chunks were
  # fetched from a non-interface peer.

  @ets_table :nfs_cross_node_test_checkouts

  @doc """
  Initialise the ETS table used by `record_checkout/4`. Idempotent —
  safe to call from a peer rpc more than once.

  ETS tables are owned by their creating process; if `init_checkout_table`
  ran in an `:rpc.call` handler the table would die when the handler
  exits (which is the moment after `init_checkout_table` returns).
  Spawn an unlinked process to own the table so it outlives the rpc.
  """
  @spec init_checkout_table() :: :ok
  def init_checkout_table do
    case :ets.info(@ets_table) do
      :undefined ->
        parent = self()
        ref = make_ref()

        spawn(fn ->
          :ets.new(@ets_table, [:named_table, :public, :duplicate_bag])
          send(parent, {:table_ready, ref})

          # Stay alive forever so the table survives. The peer node
          # tear-down at end-of-test reaps the whole VM, so no
          # explicit cleanup is needed.
          receive do
            :stop -> :ok
          end
        end)

        receive do
          {:table_ready, ^ref} -> :ok
        after
          5_000 -> :error
        end

      _ ->
        :ets.delete_all_objects(@ets_table)
        :ok
    end
  end

  @doc """
  `:telemetry` handler signature. Records the `peer` of every
  conn-pool checkout. Implemented as an append-only `:duplicate_bag`
  insert (rather than `:ets.update_counter/4`) because the `peer`
  metadata is a `{host, port}` charlist tuple — counter ops on
  arbitrary tuple keys are well-supported but the test only needs
  presence, not counts.
  """
  @spec record_checkout(list(), map(), map(), term()) :: :ok
  def record_checkout(_event, _measurements, %{peer: peer}, _config) do
    :ets.insert(@ets_table, {:checkout, peer})
    :ok
  end

  def record_checkout(_event, _measurements, _metadata, _config), do: :ok

  @doc """
  Returns the list of peers seen by `record_checkout/4`. Empty until
  at least one checkout has fired.
  """
  @spec checkout_peers() :: [term()]
  def checkout_peers do
    case :ets.info(@ets_table) do
      :undefined ->
        []

      _ ->
        @ets_table
        |> :ets.tab2list()
        |> Enum.map(fn {:checkout, peer} -> peer end)
    end
  end
end
