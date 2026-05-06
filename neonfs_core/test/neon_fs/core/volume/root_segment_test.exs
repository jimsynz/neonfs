defmodule NeonFS.Core.Volume.RootSegmentTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.HLC
  alias NeonFS.Core.Volume.RootSegment

  describe "new/1" do
    test "constructs a segment with required fields, default schedules, empty index roots" do
      seg = sample_segment()

      assert seg.format_version == RootSegment.format_version()
      assert seg.volume_id == "vol_abc"
      assert seg.volume_name == "test-volume"
      assert seg.cluster_id == "clust_xyz"
      assert seg.cluster_name == "test-cluster"
      assert seg.durability == %{type: :replicate, factor: 3, min_copies: 2}
      assert is_binary(seg.created_by_neonfs_version)
      assert seg.last_written_by_neonfs_version == seg.created_by_neonfs_version
      assert seg.on_disk_format_version == 1
      assert match?(%HLC{}, seg.hlc)
      assert seg.index_roots == %{file_index: nil, chunk_index: nil, stripe_index: nil}
      assert seg.schedules.gc.interval_ms > 0
      assert seg.schedules.scrub.interval_ms > 0
      assert seg.schedules.anti_entropy.interval_ms > 0
    end
  end

  describe "encode/1 → decode/1 round-trip" do
    test "preserves every field" do
      seg = sample_segment()
      bin = RootSegment.encode(seg)

      assert is_binary(bin)
      assert {:ok, decoded} = RootSegment.decode(bin)

      assert decoded.volume_id == seg.volume_id
      assert decoded.volume_name == seg.volume_name
      assert decoded.cluster_id == seg.cluster_id
      assert decoded.cluster_name == seg.cluster_name
      assert decoded.durability == seg.durability
      assert decoded.created_by_neonfs_version == seg.created_by_neonfs_version
      assert decoded.last_written_by_neonfs_version == seg.last_written_by_neonfs_version
      assert decoded.on_disk_format_version == seg.on_disk_format_version
      assert decoded.hlc == seg.hlc
      assert decoded.index_roots == seg.index_roots
      assert decoded.schedules == seg.schedules
    end

    test "round-trips populated index roots" do
      seg = %{
        sample_segment()
        | index_roots: %{
            file_index: <<0xAB, 0xCD>>,
            chunk_index: <<0xEF, 0x01>>,
            stripe_index: nil
          }
      }

      assert {:ok, decoded} = seg |> RootSegment.encode() |> RootSegment.decode()
      assert decoded.index_roots == seg.index_roots
    end
  end

  describe "decode/1" do
    test "rejects invalid ETF" do
      assert {:error, :invalid_etf} = RootSegment.decode(<<0, 1, 2, 3>>)
    end

    test "rejects an ETF blob that isn't a root-segment tagged tuple" do
      bin = :erlang.term_to_binary(%{some: "map"})
      assert {:error, :not_a_root_segment} = RootSegment.decode(bin)
    end

    test "rejects an unsupported format version with a clear error" do
      payload = %{
        volume_id: "v",
        volume_name: "n",
        cluster_id: "c",
        cluster_name: "cn",
        durability: %{type: :replicate, factor: 1, min_copies: 1},
        created_by_neonfs_version: "0.0.1",
        last_written_by_neonfs_version: "0.0.1",
        on_disk_format_version: 1,
        hlc: hlc_map(),
        index_roots: %{file_index: nil, chunk_index: nil, stripe_index: nil},
        schedules: %{
          gc: %{interval_ms: 86_400_000, last_run: nil},
          scrub: %{interval_ms: 7 * 86_400_000, last_run: nil},
          anti_entropy: %{interval_ms: 6 * 60 * 60 * 1_000, last_run: nil}
        }
      }

      bin = :erlang.term_to_binary({:neonfs_root_segment, 99, payload})

      assert {:error, {:unsupported_format_version, 99}} = RootSegment.decode(bin)
    end

    test "rejects payloads missing required keys" do
      bin = :erlang.term_to_binary({:neonfs_root_segment, 1, %{volume_id: "v"}})

      assert {:error, {:malformed, message}} = RootSegment.decode(bin)
      assert message =~ "missing required keys"
    end
  end

  describe "validate_cluster/2" do
    test "passes when cluster_id matches" do
      assert :ok = RootSegment.validate_cluster(sample_segment(), "clust_xyz")
    end

    test "rejects with :cluster_mismatch on mismatch" do
      assert {:error, {:cluster_mismatch, expected: "clust_other", actual: "clust_xyz"}} =
               RootSegment.validate_cluster(sample_segment(), "clust_other")
    end
  end

  describe "touch/1" do
    test "updates `last_written_by_neonfs_version` to the running daemon's version" do
      seg = %{sample_segment() | last_written_by_neonfs_version: "0.0.0"}
      touched = RootSegment.touch(seg)

      refute touched.last_written_by_neonfs_version == "0.0.0"
      assert is_binary(touched.last_written_by_neonfs_version)

      # Untouched fields stay untouched.
      assert touched.created_by_neonfs_version == seg.created_by_neonfs_version
      assert touched.volume_id == seg.volume_id
    end
  end

  ## Helpers

  defp sample_segment do
    RootSegment.new(
      volume_id: "vol_abc",
      volume_name: "test-volume",
      cluster_id: "clust_xyz",
      cluster_name: "test-cluster",
      durability: %{type: :replicate, factor: 3, min_copies: 2}
    )
  end

  defp hlc_map do
    %{
      node_id: :test@localhost,
      last_wall: 0,
      last_counter: 0,
      max_clock_skew_ms: 1_000
    }
  end
end
