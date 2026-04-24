defmodule NeonFS.Client.ChunkWriterTest do
  @moduledoc """
  Unit tests for `NeonFS.Client.ChunkWriter`.

  Stubs `NeonFS.Client.Router` and `NeonFS.Client.Discovery` so the
  writer can be exercised without a real TLS data plane or a running
  core node. Chunking is performed by the real NIF — fixed-strategy
  small inputs produce predictable chunks.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias NeonFS.Client.{ChunkWriter, Discovery, Router}
  alias NeonFS.Core.Volume

  setup :verify_on_exit!

  @target_node :core@host
  @drive_id "d1"

  defp volume_fixture(overrides \\ %{}) do
    base =
      Volume.new("test-vol",
        durability: %{type: :replicate, factor: 1, min_copies: 1},
        tiering: %{initial_tier: :hot, promotion_threshold: 10, demotion_delay: 86_400}
      )

    Map.merge(base, overrides)
  end

  defp stub_volume_lookup(volume) do
    expect(Router, :call, fn NeonFS.Core, :get_volume, ["test-vol"] -> {:ok, volume} end)
  end

  defp stub_discovery(nodes) do
    stub(Discovery, :get_core_nodes, fn -> nodes end)
  end

  describe "write_file_stream/4 — happy path" do
    test "writes a single chunk and returns an ordered ref list" do
      volume = volume_fixture()
      stub_volume_lookup(volume)
      stub_discovery([@target_node])

      data = :binary.copy(<<0xAB>>, 256)

      expect(Router, :data_call, fn @target_node, :put_chunk, args, _opts ->
        assert args[:data] == data
        assert args[:volume_id] == @drive_id
        assert args[:processing_volume_id] == volume.id
        assert args[:tier] == "hot"
        assert is_binary(args[:hash])
        :ok
      end)

      assert {:ok, [ref]} =
               ChunkWriter.write_file_stream("test-vol", "/single.bin", [data],
                 drive_id: @drive_id,
                 strategy: "fixed",
                 strategy_param: 256
               )

      assert ref.size == 256
      assert byte_size(ref.hash) == 32
      assert ref.location == %{node: @target_node, drive_id: @drive_id, tier: :hot}
    end

    test "splits a multi-chunk stream into ordered put_chunk calls" do
      volume = volume_fixture()
      stub_volume_lookup(volume)
      stub_discovery([@target_node])

      chunks = for i <- 0..3, do: :binary.copy(<<i>>, 256)
      payload = IO.iodata_to_binary(chunks)

      test_pid = self()

      stub(Router, :data_call, fn @target_node, :put_chunk, args, _opts ->
        send(test_pid, {:put, args[:hash], args[:data]})
        :ok
      end)

      assert {:ok, refs} =
               ChunkWriter.write_file_stream("test-vol", "/multi.bin", [payload],
                 drive_id: @drive_id,
                 strategy: "fixed",
                 strategy_param: 256
               )

      assert length(refs) == 4
      hashes = Enum.map(refs, & &1.hash)
      assert Enum.all?(hashes, &(byte_size(&1) == 32))

      emitted_order =
        for _ <- 1..4 do
          assert_receive {:put, hash, _data}
          hash
        end

      assert emitted_order == hashes
    end

    test "feeds multiple stream segments through the chunker incrementally" do
      volume = volume_fixture()
      stub_volume_lookup(volume)
      stub_discovery([@target_node])

      segments = [<<0::unit(8)-size(256)>>, <<1::unit(8)-size(256)>>, <<2::unit(8)-size(256)>>]

      call_counter = :counters.new(1, [])

      stub(Router, :data_call, fn @target_node, :put_chunk, _args, _opts ->
        :counters.add(call_counter, 1, 1)
        :ok
      end)

      assert {:ok, refs} =
               ChunkWriter.write_file_stream("test-vol", "/seg.bin", segments,
                 drive_id: @drive_id,
                 strategy: "fixed",
                 strategy_param: 256
               )

      assert length(refs) == 3
      assert :counters.get(call_counter, 1) == 3
    end

    test "respects the :target_node override and skips Discovery" do
      volume = volume_fixture()
      stub_volume_lookup(volume)

      stub(Discovery, :get_core_nodes, fn ->
        flunk("discovery must not be consulted when :target_node is supplied")
      end)

      pinned_node = :pinned@host

      expect(Router, :data_call, fn ^pinned_node, :put_chunk, _args, _opts -> :ok end)

      assert {:ok, [ref]} =
               ChunkWriter.write_file_stream("test-vol", "/x.bin", [<<1, 2, 3>>],
                 target_node: pinned_node,
                 strategy: "single"
               )

      assert ref.location.node == pinned_node
    end

    test "forwards the volume's tier by default but honours opts override" do
      volume =
        volume_fixture(%{
          tiering: %{initial_tier: :warm, promotion_threshold: 1, demotion_delay: 1}
        })

      stub_volume_lookup(volume)
      stub_discovery([@target_node])

      expect(Router, :data_call, fn _, :put_chunk, args, _opts ->
        assert args[:tier] == "warm"
        :ok
      end)

      assert {:ok, [_ref]} =
               ChunkWriter.write_file_stream("test-vol", "/x.bin", [<<42>>], strategy: "single")
    end
  end

  describe "write_file_stream/4 — error paths" do
    test "returns the volume lookup error when get_volume fails" do
      expect(Router, :call, fn NeonFS.Core, :get_volume, ["test-vol"] ->
        {:error, :volume_not_found}
      end)

      assert {:error, :volume_not_found} =
               ChunkWriter.write_file_stream("test-vol", "/x.bin", [<<1>>], strategy: "single")
    end

    test "returns :no_core_nodes_available when Discovery is empty" do
      volume = volume_fixture()
      stub_volume_lookup(volume)
      stub_discovery([])

      assert {:error, :no_core_nodes_available} =
               ChunkWriter.write_file_stream("test-vol", "/x.bin", [<<1>>], strategy: "single")
    end

    test "excluded nodes are skipped when selecting a target" do
      volume = volume_fixture()
      stub_volume_lookup(volume)
      stub_discovery([:bad@host, :good@host])

      expect(Router, :data_call, fn :good@host, :put_chunk, _args, _opts -> :ok end)

      assert {:ok, [_ref]} =
               ChunkWriter.write_file_stream("test-vol", "/x.bin", [<<1, 2>>],
                 exclude_nodes: [:bad@host],
                 strategy: "single"
               )
    end

    test "returns :no_core_nodes_available when every node is excluded" do
      volume = volume_fixture()
      stub_volume_lookup(volume)
      stub_discovery([:only@host])

      assert {:error, :no_core_nodes_available} =
               ChunkWriter.write_file_stream("test-vol", "/x.bin", [<<1>>],
                 exclude_nodes: [:only@host],
                 strategy: "single"
               )
    end
  end

  describe "write_file_stream/4 — abort on error" do
    test "best-effort aborts successfully-written chunks when a later put_chunk fails" do
      volume = volume_fixture()
      stub_volume_lookup(volume)
      stub_discovery([@target_node])

      payload = :binary.copy(<<0xAA>>, 256) <> :binary.copy(<<0xBB>>, 256)
      test_pid = self()

      call_counter = :counters.new(1, [])

      stub(Router, :data_call, fn @target_node, :put_chunk, args, _opts ->
        case :counters.get(call_counter, 1) do
          n when n < 1 ->
            :counters.add(call_counter, 1, 1)
            send(test_pid, {:put, args[:hash]})
            :ok

          _ ->
            send(test_pid, {:put_fail, args[:hash]})
            {:error, :boom}
        end
      end)

      abort_fn = fn target, hash -> send(test_pid, {:abort, target.node, hash}) end

      assert {:error, {:put_chunk_failed, :boom}} =
               ChunkWriter.write_file_stream("test-vol", "/fail.bin", [payload],
                 drive_id: @drive_id,
                 strategy: "fixed",
                 strategy_param: 256,
                 abort_fn: abort_fn
               )

      assert_receive {:put, first_hash}
      assert_receive {:put_fail, _}
      assert_receive {:abort, @target_node, ^first_hash}
      refute_receive {:abort, _, _}, 10
    end

    test "abort callback errors are swallowed" do
      volume = volume_fixture()
      stub_volume_lookup(volume)
      stub_discovery([@target_node])

      call_counter = :counters.new(1, [])
      test_pid = self()

      stub(Router, :data_call, fn @target_node, :put_chunk, _args, _opts ->
        case :counters.get(call_counter, 1) do
          0 ->
            :counters.add(call_counter, 1, 1)
            :ok

          _ ->
            {:error, :later_failure}
        end
      end)

      abort_fn = fn _target, _hash ->
        send(test_pid, :abort_attempted)
        raise "intentional"
      end

      payload = :binary.copy(<<0xAA>>, 256) <> :binary.copy(<<0xBB>>, 256)

      assert {:error, {:put_chunk_failed, :later_failure}} =
               ChunkWriter.write_file_stream("test-vol", "/fail.bin", [payload],
                 strategy: "fixed",
                 strategy_param: 256,
                 abort_fn: abort_fn
               )

      assert_receive :abort_attempted
    end
  end

  describe "chunk_refs_to_commit_opts/1" do
    test "extracts hashes in order, builds the locations map, and sums sizes" do
      location = %{node: @target_node, drive_id: @drive_id, tier: :hot}

      refs = [
        %{hash: <<1::256>>, location: location, size: 100},
        %{hash: <<2::256>>, location: location, size: 250},
        %{hash: <<3::256>>, location: location, size: 50}
      ]

      opts = ChunkWriter.chunk_refs_to_commit_opts(refs)

      assert opts.hashes == [<<1::256>>, <<2::256>>, <<3::256>>]
      assert opts.total_size == 400
      assert opts.locations[<<1::256>>] == [location]
      assert opts.locations[<<2::256>>] == [location]
      assert opts.locations[<<3::256>>] == [location]
    end

    test "deduplicates locations per hash when the same chunk appears twice" do
      loc_a = %{node: :a@host, drive_id: "d1", tier: :hot}
      loc_b = %{node: :b@host, drive_id: "d1", tier: :hot}

      refs = [
        %{hash: <<1::256>>, location: loc_a, size: 100},
        %{hash: <<1::256>>, location: loc_a, size: 100},
        %{hash: <<1::256>>, location: loc_b, size: 100}
      ]

      opts = ChunkWriter.chunk_refs_to_commit_opts(refs)

      assert opts.locations[<<1::256>>] == [loc_a, loc_b]
    end

    test "handles the empty ref list" do
      assert %{hashes: [], locations: %{}, total_size: 0} =
               ChunkWriter.chunk_refs_to_commit_opts([])
    end
  end
end
