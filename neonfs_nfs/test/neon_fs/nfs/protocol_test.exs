defmodule NeonFS.NFS.ProtocolTest do
  @moduledoc """
  End-to-end NFSv3 protocol tests.

  Exercises the full protocol path: Rust NFS client → TCP → nfs3_server →
  filesystem.rs → channel → Handler → mock core. Each test uses a real
  TCP connection to the NFS server started on an OS-assigned port.
  """
  use ExUnit.Case, async: false

  alias NeonFS.NFS.{Handler, InodeTable, MockCore, Native, TestClient}

  @volume "testvol"

  setup_all do
    # Start InodeTable (global, needed by Handler)
    start_supervised!(InodeTable)

    # Set up mock core with one volume
    mock = MockCore.start(volumes: [@volume])

    # Start Handler with mock core_call_fn
    {:ok, handler} =
      start_supervised({Handler, core_call_fn: mock.core_call_fn, test_notify: self()})

    # Start NFS server on port 0 (OS-assigned) with handler as callback
    {:ok, server} = Native.start_nfs_server("127.0.0.1:0", handler, 0)
    Handler.set_nfs_server(handler, server)

    # Wait for the server to bind and query the actual port
    {:ok, port} = Native.get_server_port(server)

    # Connect the test client
    {:ok, client} = TestClient.connect("127.0.0.1", port, "/")

    on_exit(fn ->
      TestClient.disconnect(client)
      Native.stop_nfs_server(server)

      if :ets.info(mock.table) != :undefined do
        MockCore.stop(mock)
      end
    end)

    {:ok, root_fh} = TestClient.root_handle(client)

    %{client: client, root_fh: root_fh, mock: mock}
  end

  describe "NULL" do
    test "ping succeeds", %{client: client} do
      assert :ok = TestClient.null(client)
    end
  end

  describe "GETATTR" do
    test "root returns directory", %{client: client, root_fh: root_fh} do
      {:ok, attrs} = TestClient.getattr(client, root_fh)
      assert attrs["file_type"] == "directory"
      assert attrs["mode"] == 0o755
    end
  end

  describe "READDIRPLUS" do
    test "root lists volumes", %{client: client, root_fh: root_fh} do
      {:ok, entries} = TestClient.readdirplus(client, root_fh)

      names = Enum.map(entries, & &1["name"])
      assert @volume in names
    end
  end

  describe "LOOKUP" do
    test "volume in root returns handle", %{client: client, root_fh: root_fh} do
      {:ok, result} = TestClient.lookup(client, root_fh, @volume)
      assert is_binary(result["handle"])
      assert byte_size(result["handle"]) > 0
    end

    test "nonexistent volume returns error", %{client: client, root_fh: root_fh} do
      assert {:error, msg} = TestClient.lookup(client, root_fh, "no_such_volume")
      assert msg =~ "NFS error"
    end
  end

  describe "volume operations" do
    setup %{client: client, root_fh: root_fh} do
      # Look up the volume to get its handle
      {:ok, result} = TestClient.lookup(client, root_fh, @volume)
      %{vol_fh: result["handle"]}
    end

    test "GETATTR on volume root succeeds", %{client: client, vol_fh: vol_fh} do
      {:ok, attrs} = TestClient.getattr(client, vol_fh)
      assert attrs["file_type"] == "directory"
    end

    test "READDIRPLUS on volume root succeeds", %{client: client, vol_fh: vol_fh} do
      {:ok, entries} = TestClient.readdirplus(client, vol_fh)
      assert is_list(entries)
    end

    test "CREATE file", %{client: client, vol_fh: vol_fh} do
      {:ok, result} = TestClient.create(client, vol_fh, "hello.txt")
      assert is_binary(result["handle"])
    end

    test "WRITE + READ round-trip", %{client: client, vol_fh: vol_fh} do
      # Create a file
      {:ok, created} = TestClient.create(client, vol_fh, "data.bin")
      fh = created["handle"]

      # Write some data
      payload = "Hello, NFS!"
      {:ok, count} = TestClient.write(client, fh, 0, payload)
      assert count == byte_size(payload)

      # Read it back
      {:ok, read_result} = TestClient.read(client, fh, 0, 1024)
      assert read_result["data"] == payload
    end

    test "MKDIR creates directory", %{client: client, vol_fh: vol_fh} do
      {:ok, result} = TestClient.mkdir(client, vol_fh, "subdir")
      assert is_binary(result["handle"])

      # Verify it appears in readdirplus
      {:ok, entries} = TestClient.readdirplus(client, vol_fh)
      names = Enum.map(entries, & &1["name"])
      assert "subdir" in names
    end

    test "REMOVE deletes file", %{client: client, vol_fh: vol_fh} do
      # Create then remove
      {:ok, _} = TestClient.create(client, vol_fh, "to_delete.txt")
      assert :ok = TestClient.remove(client, vol_fh, "to_delete.txt")

      # Verify it's gone via lookup
      assert {:error, _} = TestClient.lookup(client, vol_fh, "to_delete.txt")
    end

    test "SETATTR changes file mode", %{client: client, vol_fh: vol_fh} do
      {:ok, created} = TestClient.create(client, vol_fh, "chmod_target.txt")
      fh = created["handle"]

      {:ok, attrs} = TestClient.setattr(client, fh, mode: 0o600)
      assert attrs["mode"] == 0o600
    end

    test "SETATTR truncates file", %{client: client, vol_fh: vol_fh} do
      {:ok, created} = TestClient.create(client, vol_fh, "truncate_target.txt")
      fh = created["handle"]

      # Write some data
      {:ok, _} = TestClient.write(client, fh, 0, "Hello, World!")

      # Truncate to 0
      {:ok, attrs} = TestClient.setattr(client, fh, size: 0)
      assert attrs["size"] == 0

      # Read back should be empty
      {:ok, read_result} = TestClient.read(client, fh, 0, 1024)
      assert read_result["data"] == ""
    end

    test "RENAME moves file", %{client: client, vol_fh: vol_fh} do
      {:ok, _} = TestClient.create(client, vol_fh, "old_name.txt")

      assert :ok = TestClient.rename(client, vol_fh, "old_name.txt", vol_fh, "new_name.txt")

      # New name resolves
      {:ok, _} = TestClient.lookup(client, vol_fh, "new_name.txt")

      # Old name is gone
      assert {:error, _} = TestClient.lookup(client, vol_fh, "old_name.txt")
    end

    test "SYMLINK + READLINK round-trip", %{client: client, vol_fh: vol_fh} do
      # Create target file
      {:ok, _} = TestClient.create(client, vol_fh, "link_target.txt")

      # Create symlink
      {:ok, result} = TestClient.symlink(client, vol_fh, "my_link", "link_target.txt")
      assert is_binary(result["handle"])

      # Readlink returns the target
      {:ok, target} = TestClient.readlink(client, result["handle"])
      assert target == "link_target.txt"
    end

    test "CREATE_EXCLUSIVE succeeds then fails on duplicate", %{client: client, vol_fh: vol_fh} do
      {:ok, result} = TestClient.create_exclusive(client, vol_fh, "unique.txt")
      assert is_binary(result["handle"])

      # Second create exclusive should fail with EXIST
      assert {:error, msg} = TestClient.create_exclusive(client, vol_fh, "unique.txt")
      assert msg =~ "NFS error"
    end

    test "WRITE at non-zero offset", %{client: client, vol_fh: vol_fh} do
      {:ok, created} = TestClient.create(client, vol_fh, "offset_write.txt")
      fh = created["handle"]

      {:ok, _} = TestClient.write(client, fh, 0, "Hello")
      {:ok, _} = TestClient.write(client, fh, 5, " World")

      {:ok, read_result} = TestClient.read(client, fh, 0, 1024)
      assert read_result["data"] == "Hello World"
    end

    test "large file multi-chunk WRITE + READ", %{client: client, vol_fh: vol_fh} do
      {:ok, created} = TestClient.create(client, vol_fh, "large_file.bin")
      fh = created["handle"]

      # Generate ~2.5 MiB of deterministic data
      chunk_size = 1_048_576
      total_size = 2_621_440
      data = :crypto.strong_rand_bytes(total_size)

      # Write in 1 MiB chunks
      chunks = for offset <- 0..(total_size - 1)//chunk_size, do: offset

      Enum.each(chunks, fn offset ->
        len = min(chunk_size, total_size - offset)
        chunk = binary_part(data, offset, len)
        {:ok, count} = TestClient.write(client, fh, offset, chunk)
        assert count == len
      end)

      # Read back in 1 MiB chunks and reassemble
      read_data =
        Enum.reduce(chunks, <<>>, fn offset, acc ->
          len = min(chunk_size, total_size - offset)
          {:ok, result} = TestClient.read(client, fh, offset, len)
          acc <> result["data"]
        end)

      assert read_data == data
    end
  end
end
