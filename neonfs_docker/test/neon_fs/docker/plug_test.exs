defmodule NeonFS.Docker.PlugTest do
  use ExUnit.Case, async: true
  import Plug.Test

  alias NeonFS.Docker.{MountTracker, VolumeStore}
  alias NeonFS.Docker.Plug, as: DockerPlug

  setup do
    suffix = System.unique_integer([:positive])
    store_name = :"volume_store_#{suffix}"
    tracker_name = :"mount_tracker_#{suffix}"

    {:ok, store_pid} = VolumeStore.start_link(name: store_name)

    {:ok, tracker_pid} =
      MountTracker.start_link(
        name: tracker_name,
        mount_fn: fn vol -> {:ok, {{:mock_id, vol}, "/mnt/#{vol}"}} end,
        unmount_fn: fn _ -> :ok end
      )

    Process.unlink(tracker_pid)

    on_exit(fn ->
      if Process.alive?(store_pid), do: GenServer.stop(store_pid)

      try do
        if Process.alive?(tracker_pid), do: GenServer.stop(tracker_pid, :shutdown, 1_000)
      catch
        :exit, _ -> :ok
      end
    end)

    {:ok, store: store_name, tracker: tracker_name}
  end

  defp ok_create_fn, do: fn _name, _opts -> :ok end

  defp post(path, body, opts) do
    conn(:post, path, Jason.encode!(body))
    |> Plug.Conn.put_req_header("content-type", "application/json")
    |> Plug.Conn.put_private(:volume_store, Keyword.fetch!(opts, :store))
    |> Plug.Conn.put_private(:mount_tracker, Keyword.fetch!(opts, :tracker))
    |> Plug.Conn.put_private(:core_create_fn, Keyword.get(opts, :core_create_fn, ok_create_fn()))
    |> DockerPlug.call(DockerPlug.init([]))
  end

  defp decode(conn), do: Jason.decode!(conn.resp_body)

  describe "POST /Plugin.Activate" do
    test "advertises VolumeDriver", %{store: store, tracker: tracker} do
      conn = post("/Plugin.Activate", %{}, store: store, tracker: tracker)
      assert conn.status == 200
      assert decode(conn) == %{"Implements" => ["VolumeDriver"]}
    end
  end

  describe "POST /VolumeDriver.Capabilities" do
    test "returns scope: local", %{store: store, tracker: tracker} do
      conn = post("/VolumeDriver.Capabilities", %{}, store: store, tracker: tracker)
      assert conn.status == 200
      assert decode(conn) == %{"Capabilities" => %{"Scope" => "local"}}
    end
  end

  describe "POST /VolumeDriver.Create" do
    test "records the volume locally and returns empty Err", %{store: store, tracker: tracker} do
      conn =
        post("/VolumeDriver.Create", %{"Name" => "vol-a", "Opts" => %{"durability" => "2"}},
          store: store,
          tracker: tracker
        )

      assert conn.status == 200
      assert decode(conn) == %{"Err" => ""}

      # The local store keeps the raw `-o` map verbatim so
      # `docker volume inspect` round-trips it.
      assert {:ok, %{name: "vol-a", opts: %{"durability" => "2"}}} =
               VolumeStore.get(store, "vol-a")
    end

    test "propagates a parsed kw list to core_create_fn", %{store: store, tracker: tracker} do
      parent = self()

      fun = fn name, opts ->
        send(parent, {:core_create_called, name, opts})
        :ok
      end

      conn =
        post(
          "/VolumeDriver.Create",
          %{
            "Name" => "vol-b",
            "Opts" => %{"owner" => "alice", "atime_mode" => "relatime"}
          },
          store: store,
          tracker: tracker,
          core_create_fn: fun
        )

      assert conn.status == 200
      assert decode(conn) == %{"Err" => ""}

      # `core_create_fn` now receives the typed kw list — strings have
      # been coerced (atime_mode → atom). #583.
      assert_received {:core_create_called, "vol-b", parsed}
      assert Keyword.get(parsed, :owner) == "alice"
      assert Keyword.get(parsed, :atime_mode) == :relatime
    end

    test "rejects unknown opts with an Err", %{store: store, tracker: tracker} do
      conn =
        post("/VolumeDriver.Create", %{"Name" => "vol-x", "Opts" => %{"bogus" => "1"}},
          store: store,
          tracker: tracker
        )

      assert conn.status == 200
      assert %{"Err" => err} = decode(conn)
      assert err =~ "unknown docker volume opt: bogus"

      # Volume must NOT have been recorded locally.
      assert {:error, :not_found} = VolumeStore.get(store, "vol-x")
    end

    test "rejects malformed values with an Err", %{store: store, tracker: tracker} do
      conn =
        post(
          "/VolumeDriver.Create",
          %{"Name" => "vol-y", "Opts" => %{"io_weight" => "not-an-int"}},
          store: store,
          tracker: tracker
        )

      assert conn.status == 200
      assert %{"Err" => err} = decode(conn)
      assert err =~ "io_weight must be a positive integer"
      assert {:error, :not_found} = VolumeStore.get(store, "vol-y")
    end

    test "durability=N translates to a replicated config map", %{store: store, tracker: tracker} do
      parent = self()

      fun = fn _name, opts ->
        send(parent, {:opts, opts})
        :ok
      end

      conn =
        post("/VolumeDriver.Create", %{"Name" => "vol-z", "Opts" => %{"durability" => "5"}},
          store: store,
          tracker: tracker,
          core_create_fn: fun
        )

      assert conn.status == 200
      assert decode(conn) == %{"Err" => ""}

      assert_received {:opts, opts}
      # ⌈5/2⌉ = 3 → majority quorum.
      assert Keyword.get(opts, :durability) == %{type: :replicate, factor: 5, min_copies: 3}
    end

    test "reports core create failure in Err", %{store: store, tracker: tracker} do
      fun = fn _name, _opts -> {:error, "boom"} end

      conn =
        post("/VolumeDriver.Create", %{"Name" => "vol-c"},
          store: store,
          tracker: tracker,
          core_create_fn: fun
        )

      assert conn.status == 200
      assert %{"Err" => err} = decode(conn)
      assert err == "boom"
      assert {:error, :not_found} = VolumeStore.get(store, "vol-c")
    end

    test "handles missing Opts as empty map", %{store: store, tracker: tracker} do
      conn = post("/VolumeDriver.Create", %{"Name" => "vol-d"}, store: store, tracker: tracker)

      assert conn.status == 200
      assert {:ok, %{name: "vol-d", opts: %{}}} = VolumeStore.get(store, "vol-d")
    end

    test "rejects missing Name with Err", %{store: store, tracker: tracker} do
      conn = post("/VolumeDriver.Create", %{}, store: store, tracker: tracker)
      assert conn.status == 200
      assert %{"Err" => "missing or invalid Name"} = decode(conn)
    end
  end

  describe "POST /VolumeDriver.Remove" do
    test "drops the local record and returns empty Err", %{store: store, tracker: tracker} do
      :ok = VolumeStore.put(store, "vol-to-remove", %{})

      conn =
        post("/VolumeDriver.Remove", %{"Name" => "vol-to-remove"}, store: store, tracker: tracker)

      assert conn.status == 200
      assert decode(conn) == %{"Err" => ""}
      assert {:error, :not_found} = VolumeStore.get(store, "vol-to-remove")
    end

    test "idempotent for unknown name", %{store: store, tracker: tracker} do
      conn =
        post("/VolumeDriver.Remove", %{"Name" => "never-created"}, store: store, tracker: tracker)

      assert conn.status == 200
      assert decode(conn) == %{"Err" => ""}
    end

    test "rejects missing Name", %{store: store, tracker: tracker} do
      conn = post("/VolumeDriver.Remove", %{}, store: store, tracker: tracker)
      assert %{"Err" => "missing or invalid Name"} = decode(conn)
    end
  end

  describe "POST /VolumeDriver.Get" do
    test "returns the volume record for a known name", %{store: store, tracker: tracker} do
      :ok = VolumeStore.put(store, "vol-x", %{})

      conn = post("/VolumeDriver.Get", %{"Name" => "vol-x"}, store: store, tracker: tracker)

      assert conn.status == 200

      assert %{"Volume" => %{"Name" => "vol-x", "Mountpoint" => ""}, "Err" => ""} =
               decode(conn)
    end

    test "returns Err for unknown name", %{store: store, tracker: tracker} do
      conn = post("/VolumeDriver.Get", %{"Name" => "missing"}, store: store, tracker: tracker)
      assert conn.status == 200
      assert %{"Err" => "volume not found"} = decode(conn)
    end
  end

  describe "POST /VolumeDriver.List" do
    test "lists all recorded volumes", %{store: store, tracker: tracker} do
      :ok = VolumeStore.put(store, "vol-1", %{})
      :ok = VolumeStore.put(store, "vol-2", %{})

      conn = post("/VolumeDriver.List", %{}, store: store, tracker: tracker)

      assert conn.status == 200
      assert %{"Volumes" => vols, "Err" => ""} = decode(conn)
      names = vols |> Enum.map(& &1["Name"]) |> Enum.sort()
      assert names == ["vol-1", "vol-2"]
    end

    test "returns empty list when store is empty", %{store: store, tracker: tracker} do
      conn = post("/VolumeDriver.List", %{}, store: store, tracker: tracker)
      assert decode(conn) == %{"Volumes" => [], "Err" => ""}
    end
  end

  describe "POST /VolumeDriver.Path" do
    test "returns empty Mountpoint before Mount", %{store: store, tracker: tracker} do
      conn = post("/VolumeDriver.Path", %{"Name" => "anything"}, store: store, tracker: tracker)
      assert conn.status == 200
      assert decode(conn) == %{"Mountpoint" => "", "Err" => ""}
    end

    test "returns the active mountpoint after Mount", %{store: store, tracker: tracker} do
      post("/VolumeDriver.Mount", %{"Name" => "vol-p"}, store: store, tracker: tracker)
      conn = post("/VolumeDriver.Path", %{"Name" => "vol-p"}, store: store, tracker: tracker)

      assert conn.status == 200
      assert decode(conn) == %{"Mountpoint" => "/mnt/vol-p", "Err" => ""}
    end

    test "rejects missing Name", %{store: store, tracker: tracker} do
      conn = post("/VolumeDriver.Path", %{}, store: store, tracker: tracker)
      assert %{"Err" => "missing or invalid Name"} = decode(conn)
    end
  end

  describe "POST /VolumeDriver.Mount" do
    test "returns the plugin-allocated mountpoint and empty Err", %{
      store: store,
      tracker: tracker
    } do
      conn = post("/VolumeDriver.Mount", %{"Name" => "vol-a"}, store: store, tracker: tracker)
      assert conn.status == 200
      assert decode(conn) == %{"Mountpoint" => "/mnt/vol-a", "Err" => ""}
    end

    test "ref-counts concurrent Mount calls for the same volume", %{
      store: store,
      tracker: tracker
    } do
      conn1 = post("/VolumeDriver.Mount", %{"Name" => "vol-b"}, store: store, tracker: tracker)
      conn2 = post("/VolumeDriver.Mount", %{"Name" => "vol-b"}, store: store, tracker: tracker)

      assert decode(conn1) == %{"Mountpoint" => "/mnt/vol-b", "Err" => ""}
      assert decode(conn2) == %{"Mountpoint" => "/mnt/vol-b", "Err" => ""}
      assert [{"vol-b", %{ref_count: 2}}] = MountTracker.list(tracker)
    end

    test "reports mount_fn errors in Err", %{store: store} do
      failing_tracker = :"failing_tracker_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        MountTracker.start_link(
          name: failing_tracker,
          mount_fn: fn _ -> {:error, :permission_denied} end,
          unmount_fn: fn _ -> :ok end
        )

      Process.unlink(pid)

      on_exit(fn ->
        try do
          if Process.alive?(pid), do: GenServer.stop(pid, :shutdown, 1_000)
        catch
          :exit, _ -> :ok
        end
      end)

      conn =
        post("/VolumeDriver.Mount", %{"Name" => "vol-x"},
          store: store,
          tracker: failing_tracker
        )

      assert %{"Err" => err} = decode(conn)
      assert err =~ "permission_denied"
    end

    test "rejects missing Name", %{store: store, tracker: tracker} do
      conn = post("/VolumeDriver.Mount", %{}, store: store, tracker: tracker)
      assert %{"Err" => "missing or invalid Name"} = decode(conn)
    end

    test "returns a clear Err message when MountTracker refuses :mount_pool_full",
         %{store: store} do
      capped_tracker = :"capped_tracker_#{System.unique_integer([:positive])}"

      {:ok, pid} =
        MountTracker.start_link(
          name: capped_tracker,
          mount_fn: fn name -> {:ok, {{:mount_id, name}, "/tmp/" <> name}} end,
          unmount_fn: fn _ -> :ok end,
          max_mounts: 0
        )

      Process.unlink(pid)

      on_exit(fn ->
        try do
          if Process.alive?(pid), do: GenServer.stop(pid, :shutdown, 1_000)
        catch
          :exit, _ -> :ok
        end
      end)

      conn =
        post("/VolumeDriver.Mount", %{"Name" => "vol-overflow"},
          store: store,
          tracker: capped_tracker
        )

      assert %{"Err" => err} = decode(conn)
      assert err =~ "mount pool full"
      assert err =~ ":max_mounts"
    end
  end

  describe "POST /VolumeDriver.Unmount" do
    test "unmounts the volume after the last reference releases", %{
      store: store,
      tracker: tracker
    } do
      post("/VolumeDriver.Mount", %{"Name" => "vol-u"}, store: store, tracker: tracker)
      conn = post("/VolumeDriver.Unmount", %{"Name" => "vol-u"}, store: store, tracker: tracker)

      assert decode(conn) == %{"Err" => ""}
      assert MountTracker.list(tracker) == []
    end

    test "keeps the mount alive until every reference releases", %{
      store: store,
      tracker: tracker
    } do
      post("/VolumeDriver.Mount", %{"Name" => "vol-u2"}, store: store, tracker: tracker)
      post("/VolumeDriver.Mount", %{"Name" => "vol-u2"}, store: store, tracker: tracker)
      post("/VolumeDriver.Unmount", %{"Name" => "vol-u2"}, store: store, tracker: tracker)

      assert [{"vol-u2", %{ref_count: 1}}] = MountTracker.list(tracker)
    end

    test "is idempotent for volumes that were never mounted", %{store: store, tracker: tracker} do
      conn =
        post("/VolumeDriver.Unmount", %{"Name" => "never-mounted"},
          store: store,
          tracker: tracker
        )

      assert decode(conn) == %{"Err" => ""}
    end

    test "rejects missing Name", %{store: store, tracker: tracker} do
      conn = post("/VolumeDriver.Unmount", %{}, store: store, tracker: tracker)
      assert %{"Err" => "missing or invalid Name"} = decode(conn)
    end
  end

  describe "unknown paths" do
    test "return 404 with Err", %{store: store, tracker: tracker} do
      conn = post("/bogus", %{}, store: store, tracker: tracker)
      assert conn.status == 404
      assert %{"Err" => "not found"} = decode(conn)
    end
  end

  describe "GET /health" do
    test "returns 503 with body when no checks are healthy" do
      conn =
        :get
        |> conn("/health")
        |> Plug.Conn.put_private(:health_checks,
          stub: fn -> %{status: :unhealthy, reason: :stub} end
        )
        |> DockerPlug.call(DockerPlug.init([]))

      assert conn.status == 503
      assert %{"status" => "unhealthy", "checks" => checks} = decode(conn)
      assert is_map(checks)
    end

    test "returns 200 when every check is healthy" do
      conn =
        :get
        |> conn("/health")
        |> Plug.Conn.put_private(:health_checks, stub: fn -> %{status: :healthy} end)
        |> DockerPlug.call(DockerPlug.init([]))

      assert conn.status == 200
      assert %{"status" => "healthy"} = decode(conn)
    end
  end
end
