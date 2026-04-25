defmodule NeonFS.Core.DRSnapshotTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{DRSnapshot, KVStore, RaServer, SystemVolume, VolumeRegistry}

  @moduletag :tmp_dir

  describe "serialise_index/1" do
    test "round-trips an empty map to a SHA-256 of the empty string" do
      {iolist, hasher, total} = DRSnapshot.serialise_index(%{})
      assert IO.iodata_to_binary(iolist) == <<>>
      assert total == 0
      empty_sha = :crypto.hash(:sha256, <<>>)
      assert :crypto.hash_final(hasher) == empty_sha
    end

    test "round-trips a small map through deserialise_index/1" do
      input = %{"a" => 1, "b" => "two", "c" => %{nested: true}}
      {iolist, hasher, total} = DRSnapshot.serialise_index(input)
      bytes = IO.iodata_to_binary(iolist)

      assert byte_size(bytes) == total
      assert byte_size(:crypto.hash_final(hasher)) == 32

      {:ok, entries} = DRSnapshot.deserialise_index(bytes)
      assert Enum.sort(entries) == Enum.sort(Map.to_list(input))
    end

    test "stable hash across runs (same input → same digest)" do
      input = %{"k1" => 1, "k2" => 2, "k3" => "three"}
      {_, hasher_a, _} = DRSnapshot.serialise_index(input)
      {_, hasher_b, _} = DRSnapshot.serialise_index(input)
      assert :crypto.hash_final(hasher_a) == :crypto.hash_final(hasher_b)
    end

    test "different inputs produce different digests" do
      a = DRSnapshot.serialise_index(%{"a" => 1}) |> elem(1) |> :crypto.hash_final()
      b = DRSnapshot.serialise_index(%{"a" => 2}) |> elem(1) |> :crypto.hash_final()
      assert a != b
    end
  end

  describe "deserialise_index/1" do
    test "returns :malformed for trailing garbage" do
      garbage = <<255, 255, 255, 255, "xx">>
      assert {:error, :malformed} = DRSnapshot.deserialise_index(garbage)
    end

    test "returns :malformed for an unparseable ETF segment" do
      garbage = <<2::big-32, 0xFF, 0xFE>>
      assert {:error, :malformed} = DRSnapshot.deserialise_index(garbage)
    end
  end

  describe "create/1" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()

      master_key = :crypto.strong_rand_bytes(32) |> Base.encode64()
      write_cluster_json(tmp_dir, master_key)

      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()

      {:ok, _vol} = VolumeRegistry.create_system_volume()

      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "writes manifest + per-index files for the captured state" do
      state = %{
        version: 42,
        chunks: %{"chunk-1" => %{size: 4096}},
        files: %{"file-1" => %{path: "/a.txt", size: 13}},
        stripes: %{},
        volumes: %{"vol-1" => %{name: "data"}},
        services: %{},
        segment_assignments: %{},
        encryption_keys: %{},
        volume_acls: %{},
        s3_credentials: %{},
        escalations: %{},
        kv: %{}
      }

      assert {:ok, %{path: dir, manifest: manifest}} =
               DRSnapshot.create(state: state, timestamp: "20260425T120000Z")

      assert dir == "/dr/20260425T120000Z"

      # Every captured index must land as a file.
      for index <- ~w(chunks files stripes volumes services segment_assignments
                      encryption_keys volume_acls s3_credentials escalations kv)a do
        path = Path.join(dir, "#{index}.snapshot")
        assert SystemVolume.exists?(path), "expected #{path} to exist"
      end

      # Manifest links every file with size + sha256.
      assert manifest.version == 1
      assert manifest.created_at == "20260425T120000Z"
      assert manifest.state_version == 42

      assert Enum.all?(manifest.files, fn entry ->
               is_binary(entry.path) and is_integer(entry.bytes) and
                 is_binary(entry.sha256) and entry.kind in [:index, :ca]
             end)

      # The captured `files` index round-trips back to the input.
      {:ok, content} = SystemVolume.read(Path.join(dir, "files.snapshot"))
      assert {:ok, decoded} = DRSnapshot.deserialise_index(content)
      assert decoded == [{"file-1", %{path: "/a.txt", size: 13}}]
    end

    test "manifest.json is valid JSON and listed at the snapshot root" do
      state = %{version: 0, chunks: %{}, files: %{}, stripes: %{}, volumes: %{}}

      {:ok, %{path: dir}} =
        DRSnapshot.create(state: state, timestamp: "20260425T130000Z")

      assert {:ok, body} = SystemVolume.read(Path.join(dir, "manifest.json"))
      assert {:ok, parsed} = Jason.decode(body)
      assert parsed["version"] == 1
      assert parsed["created_at"] == "20260425T130000Z"
      assert is_list(parsed["files"])
    end

    test "manifest entry sha256 matches the actual file digest" do
      state = %{
        version: 1,
        chunks: %{"c1" => %{size: 8}, "c2" => %{size: 16}},
        files: %{},
        stripes: %{},
        volumes: %{}
      }

      {:ok, %{path: dir, manifest: manifest}} =
        DRSnapshot.create(state: state, timestamp: "20260425T140000Z")

      chunks_entry = Enum.find(manifest.files, &(Path.basename(&1.path) == "chunks.snapshot"))
      refute is_nil(chunks_entry)

      {:ok, content} = SystemVolume.read(Path.join(dir, "chunks.snapshot"))
      assert byte_size(content) == chunks_entry.bytes

      assert :crypto.hash(:sha256, content) |> Base.encode16(case: :lower) ==
               chunks_entry.sha256
    end

    test "captures CA material from /tls/* when present" do
      :ok = SystemVolume.write("/tls/ca.crt", "fake-ca-cert-pem")
      :ok = SystemVolume.write("/tls/serial", "42")

      state = %{version: 1, chunks: %{}, files: %{}, stripes: %{}, volumes: %{}}

      {:ok, %{path: dir, manifest: manifest}} =
        DRSnapshot.create(state: state, timestamp: "20260425T150000Z")

      ca_entries = Enum.filter(manifest.files, &(&1.kind == :ca))
      assert length(ca_entries) == 2

      assert {:ok, "fake-ca-cert-pem"} = SystemVolume.read(Path.join(dir, "ca/ca.crt"))
      assert {:ok, "42"} = SystemVolume.read(Path.join(dir, "ca/serial"))
    end

    test "missing CA files are silently skipped (no error, no entry)" do
      state = %{version: 1, chunks: %{}, files: %{}, stripes: %{}, volumes: %{}}

      {:ok, %{manifest: manifest}} =
        DRSnapshot.create(state: state, timestamp: "20260425T160000Z")

      assert Enum.all?(manifest.files, &(&1.kind == :index))
    end
  end

  describe "create/1 against a real Ra state machine" do
    @describetag :integration_ra

    setup %{tmp_dir: tmp_dir} do
      ensure_node_named()
      configure_test_dirs(tmp_dir)
      stop_ra()

      master_key = :crypto.strong_rand_bytes(32) |> Base.encode64()
      write_cluster_json(tmp_dir, master_key)

      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()
      start_ra()

      case RaServer.init_cluster() do
        :ok -> :ok
        {:error, :already_exists} -> :ok
        other -> raise "init_cluster failed: #{inspect(other)}"
      end

      case VolumeRegistry.create_system_volume() do
        {:ok, _vol} -> :ok
        {:error, :already_exists} -> :ok
        other -> raise "create_system_volume failed: #{inspect(other)}"
      end

      on_exit(fn ->
        stop_ra()
        cleanup_test_dirs()
      end)

      :ok
    end

    test "captures the live committed Ra state via local_query" do
      # Submit a couple of commands so the live state is non-empty.
      :ok = KVStore.put("dr-test-key", "dr-test-value")

      assert {:ok, %{path: dir, manifest: manifest}} =
               DRSnapshot.create(timestamp: "20260425T180000Z")

      assert manifest.state_version > 0

      {:ok, kv_bytes} = SystemVolume.read(Path.join(dir, "kv.snapshot"))
      {:ok, decoded} = DRSnapshot.deserialise_index(kv_bytes)
      assert {"dr-test-key", "dr-test-value"} in decoded
    end
  end

  describe "create/1 without a system volume" do
    setup %{tmp_dir: tmp_dir} do
      configure_test_dirs(tmp_dir)
      stop_ra()

      master_key = :crypto.strong_rand_bytes(32) |> Base.encode64()
      write_cluster_json(tmp_dir, master_key)

      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()

      # Deliberately skip `create_system_volume/0` so the snapshot's
      # `_system` lookup fails.
      on_exit(fn -> cleanup_test_dirs() end)
      :ok
    end

    test "surfaces :system_volume_not_found when the volume isn't provisioned" do
      assert {:error, :system_volume_not_found} =
               DRSnapshot.create(
                 state: %{version: 0, chunks: %{}, files: %{}, stripes: %{}, volumes: %{}},
                 timestamp: "20260425T170000Z"
               )
    end
  end

  describe "list/0 + get/1 against a real Ra state machine" do
    @describetag :integration_ra

    setup %{tmp_dir: tmp_dir} do
      ensure_node_named()
      configure_test_dirs(tmp_dir)
      stop_ra()

      master_key = :crypto.strong_rand_bytes(32) |> Base.encode64()
      write_cluster_json(tmp_dir, master_key)

      start_drive_registry()
      start_blob_store()
      start_chunk_index()
      start_file_index()
      start_stripe_index()
      start_volume_registry()
      ensure_chunk_access_tracker()
      start_ra()

      case RaServer.init_cluster() do
        :ok -> :ok
        {:error, :already_exists} -> :ok
        other -> raise "init_cluster failed: #{inspect(other)}"
      end

      case VolumeRegistry.create_system_volume() do
        {:ok, _vol} -> :ok
        {:error, :already_exists} -> :ok
        other -> raise "create_system_volume failed: #{inspect(other)}"
      end

      on_exit(fn ->
        stop_ra()
        cleanup_test_dirs()
      end)

      :ok
    end

    test "list returns every snapshot newest-first with manifest already decoded" do
      {:ok, _} = DRSnapshot.create(timestamp: "20260101T000000Z")
      {:ok, _} = DRSnapshot.create(timestamp: "20260201T000000Z")

      assert {:ok, snapshots} = DRSnapshot.list()

      ids = Enum.map(snapshots, & &1.id)
      assert ids == ["20260201T000000Z", "20260101T000000Z"]

      Enum.each(snapshots, fn s ->
        assert s.path =~ "/dr/" <> s.id
        assert is_map(s.manifest)
        assert s.manifest.version == 1
        assert is_list(s.manifest.files)
        assert Enum.all?(s.manifest.files, fn f -> f.kind in [:index, :ca] end)
      end)
    end

    test "get returns a single snapshot by id with the same manifest shape as list" do
      {:ok, _} = DRSnapshot.create(timestamp: "20260301T120000Z")

      assert {:ok, snapshot} = DRSnapshot.get("20260301T120000Z")
      assert snapshot.id == "20260301T120000Z"
      assert snapshot.manifest.version == 1
      assert is_integer(snapshot.manifest.state_version)
      refute snapshot.manifest.files == []
    end

    test "get returns :not_found for an unknown id" do
      assert {:error, :not_found} = DRSnapshot.get("00000000T000000Z")
    end
  end
end
