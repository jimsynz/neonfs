defmodule NeonFS.Core.GCWithSnapshotsTest do
  @moduledoc """
  Exercises the multi-root GC's interaction with the real Ra-backed
  `Snapshot` module — `GarbageCollector.collect/1` should consult
  `Snapshot.list/1` (or its global equivalent) via the default
  enumerator and surface each snapshot's `root_chunk_hash` to the
  injected `:metadata_reader`.

  The mark-phase correctness (chunks/stripes from snapshot trees
  are preserved) is covered by `NeonFS.Core.GarbageCollectorTest`'s
  `"collect/1 with snapshots — multi-root mark"` describe — those
  tests inject the enumerator directly. This file exercises the
  *default* enumerator path through real Ra.

  Full file-write → snapshot → delete → collect end-to-end remains
  to be wired once the integration-test scaffolding around real
  drive provisioning lands (deferred to a follow-up).
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{GarbageCollector, RaServer, RaSupervisor, Snapshot}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()
    # `collect/1`'s sweep walks the chunk/file/stripe ETS tables; the
    # subsystems own them. Start them post-Ra so they pick up the
    # running cluster.
    start_core_subsystems()
    start_stripe_index()
    on_exit(fn -> cleanup_test_dirs() end)
    :ok
  end

  defp register_volume_root(volume_id, root_chunk_hash) do
    entry = %{
      volume_id: volume_id,
      root_chunk_hash: root_chunk_hash,
      drive_locations: [],
      durability_cache: %{},
      updated_at: DateTime.utc_now()
    }

    {:ok, :ok, _leader} = RaSupervisor.command({:register_volume_root, entry})
    :ok
  end

  describe "default snapshot enumerator" do
    test "passes each snapshot's root_chunk_hash to the reader (volume-scoped)" do
      hash_one = <<0xAA, 0xBB>>
      hash_two = <<0xCC, 0xDD>>

      :ok = register_volume_root("vol-1", hash_one)
      {:ok, _snap_one} = Snapshot.create("vol-1", name: "first")

      :ok = register_volume_root("vol-1", hash_two)
      {:ok, _snap_two} = Snapshot.create("vol-1", name: "second")

      table = :ets.new(:reader_calls, [:public, :duplicate_bag])
      :persistent_term.put({__MODULE__.RecordingReader, :table}, table)

      try do
        assert {:ok, _} =
                 GarbageCollector.collect(
                   volume_id: "vol-1",
                   metadata_reader: __MODULE__.RecordingReader
                 )

        roots =
          :ets.tab2list(table)
          |> Enum.map(fn {:range, root} -> root end)
          |> MapSet.new()

        assert MapSet.equal?(roots, MapSet.new([hash_one, hash_two]))
      after
        :persistent_term.erase({__MODULE__.RecordingReader, :table})
        :ets.delete(table)
      end
    end

    test "scopes snapshot enumeration to the requested volume" do
      :ok = register_volume_root("vol-keep", <<1>>)
      :ok = register_volume_root("vol-skip", <<2>>)
      {:ok, _} = Snapshot.create("vol-keep")
      {:ok, _} = Snapshot.create("vol-skip")

      table = :ets.new(:reader_calls, [:public, :duplicate_bag])
      :persistent_term.put({__MODULE__.RecordingReader, :table}, table)

      try do
        assert {:ok, _} =
                 GarbageCollector.collect(
                   volume_id: "vol-keep",
                   metadata_reader: __MODULE__.RecordingReader
                 )

        roots =
          :ets.tab2list(table)
          |> Enum.map(fn {:range, root} -> root end)

        assert roots == [<<1>>]
      after
        :persistent_term.erase({__MODULE__.RecordingReader, :table})
        :ets.delete(table)
      end
    end

    test "global pass enumerates snapshots from every volume" do
      :ok = register_volume_root("vol-a", <<10>>)
      :ok = register_volume_root("vol-b", <<20>>)
      {:ok, _} = Snapshot.create("vol-a")
      {:ok, _} = Snapshot.create("vol-b")

      table = :ets.new(:reader_calls, [:public, :duplicate_bag])
      :persistent_term.put({__MODULE__.RecordingReader, :table}, table)

      try do
        assert {:ok, _} =
                 GarbageCollector.collect(metadata_reader: __MODULE__.RecordingReader)

        roots =
          :ets.tab2list(table)
          |> Enum.map(fn {:range, root} -> root end)
          |> MapSet.new()

        assert MapSet.equal?(roots, MapSet.new([<<10>>, <<20>>]))
      after
        :persistent_term.erase({__MODULE__.RecordingReader, :table})
        :ets.delete(table)
      end
    end

    test "no snapshots → reader is never consulted" do
      :ok = register_volume_root("vol-empty", <<99>>)

      assert {:ok, _} =
               GarbageCollector.collect(
                 volume_id: "vol-empty",
                 metadata_reader: __MODULE__.RaisingReader
               )
    end
  end

  defmodule RecordingReader do
    @moduledoc false

    def range(_volume_id, :file_index, _start, _end_, opts) do
      table = :persistent_term.get({__MODULE__, :table})
      root = Keyword.fetch!(opts, :at_root)
      :ets.insert(table, {:range, root})
      {:ok, []}
    end

    def get_stripe(_volume_id, _key, _opts), do: {:error, :not_found}
  end

  defmodule RaisingReader do
    @moduledoc false

    def range(_, _, _, _, _),
      do: raise("RaisingReader.range/5 must not be called when no snapshots exist")

    def get_stripe(_, _, _),
      do: raise("RaisingReader.get_stripe/3 must not be called when no snapshots exist")
  end
end
