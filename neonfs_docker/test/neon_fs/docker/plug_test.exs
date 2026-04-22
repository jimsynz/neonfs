defmodule NeonFS.Docker.PlugTest do
  use ExUnit.Case, async: true
  import Plug.Test

  alias NeonFS.Docker.Plug, as: DockerPlug
  alias NeonFS.Docker.VolumeStore

  setup do
    # Each test gets its own isolated volume store.
    name = :"volume_store_#{System.unique_integer([:positive])}"
    {:ok, pid} = VolumeStore.start_link(name: name)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    {:ok, store: name}
  end

  defp ok_create_fn, do: fn _name, _opts -> :ok end

  defp post(path, body, opts) do
    conn(:post, path, Jason.encode!(body))
    |> Plug.Conn.put_req_header("content-type", "application/json")
    |> Plug.Conn.put_private(:volume_store, Keyword.fetch!(opts, :store))
    |> Plug.Conn.put_private(:core_create_fn, Keyword.get(opts, :core_create_fn, ok_create_fn()))
    |> DockerPlug.call(DockerPlug.init([]))
  end

  defp decode(conn), do: Jason.decode!(conn.resp_body)

  describe "POST /Plugin.Activate" do
    test "advertises VolumeDriver", %{store: store} do
      conn = post("/Plugin.Activate", %{}, store: store)
      assert conn.status == 200
      assert decode(conn) == %{"Implements" => ["VolumeDriver"]}
    end
  end

  describe "POST /VolumeDriver.Capabilities" do
    test "returns scope: local", %{store: store} do
      conn = post("/VolumeDriver.Capabilities", %{}, store: store)
      assert conn.status == 200
      assert decode(conn) == %{"Capabilities" => %{"Scope" => "local"}}
    end
  end

  describe "POST /VolumeDriver.Create" do
    test "records the volume locally and returns empty Err", %{store: store} do
      conn =
        post("/VolumeDriver.Create", %{"Name" => "vol-a", "Opts" => %{"replication" => "2"}},
          store: store
        )

      assert conn.status == 200
      assert decode(conn) == %{"Err" => ""}

      assert {:ok, %{name: "vol-a", opts: %{"replication" => "2"}}} =
               VolumeStore.get(store, "vol-a")
    end

    test "propagates to core via core_create_fn", %{store: store} do
      parent = self()

      fun = fn name, opts ->
        send(parent, {:core_create_called, name, opts})
        :ok
      end

      conn =
        post("/VolumeDriver.Create", %{"Name" => "vol-b", "Opts" => %{"k" => "v"}},
          store: store,
          core_create_fn: fun
        )

      assert conn.status == 200
      assert_received {:core_create_called, "vol-b", %{"k" => "v"}}
      assert decode(conn) == %{"Err" => ""}
    end

    test "reports core create failure in Err", %{store: store} do
      fun = fn _name, _opts -> {:error, "boom"} end
      conn = post("/VolumeDriver.Create", %{"Name" => "vol-c"}, store: store, core_create_fn: fun)

      assert conn.status == 200
      assert %{"Err" => err} = decode(conn)
      assert err == "boom"
      assert {:error, :not_found} = VolumeStore.get(store, "vol-c")
    end

    test "handles missing Opts as empty map", %{store: store} do
      conn = post("/VolumeDriver.Create", %{"Name" => "vol-d"}, store: store)

      assert conn.status == 200
      assert {:ok, %{name: "vol-d", opts: %{}}} = VolumeStore.get(store, "vol-d")
    end

    test "rejects missing Name with Err", %{store: store} do
      conn = post("/VolumeDriver.Create", %{}, store: store)
      assert conn.status == 200
      assert %{"Err" => "missing or invalid Name"} = decode(conn)
    end
  end

  describe "POST /VolumeDriver.Remove" do
    test "drops the local record and returns empty Err", %{store: store} do
      :ok = VolumeStore.put(store, "vol-to-remove", %{})

      conn = post("/VolumeDriver.Remove", %{"Name" => "vol-to-remove"}, store: store)

      assert conn.status == 200
      assert decode(conn) == %{"Err" => ""}
      assert {:error, :not_found} = VolumeStore.get(store, "vol-to-remove")
    end

    test "idempotent for unknown name", %{store: store} do
      conn = post("/VolumeDriver.Remove", %{"Name" => "never-created"}, store: store)
      assert conn.status == 200
      assert decode(conn) == %{"Err" => ""}
    end

    test "rejects missing Name", %{store: store} do
      conn = post("/VolumeDriver.Remove", %{}, store: store)
      assert %{"Err" => "missing or invalid Name"} = decode(conn)
    end
  end

  describe "POST /VolumeDriver.Get" do
    test "returns the volume record for a known name", %{store: store} do
      :ok = VolumeStore.put(store, "vol-x", %{})

      conn = post("/VolumeDriver.Get", %{"Name" => "vol-x"}, store: store)

      assert conn.status == 200

      assert %{"Volume" => %{"Name" => "vol-x", "Mountpoint" => ""}, "Err" => ""} =
               decode(conn)
    end

    test "returns Err for unknown name", %{store: store} do
      conn = post("/VolumeDriver.Get", %{"Name" => "missing"}, store: store)
      assert conn.status == 200
      assert %{"Err" => "volume not found"} = decode(conn)
    end
  end

  describe "POST /VolumeDriver.List" do
    test "lists all recorded volumes", %{store: store} do
      :ok = VolumeStore.put(store, "vol-1", %{})
      :ok = VolumeStore.put(store, "vol-2", %{})

      conn = post("/VolumeDriver.List", %{}, store: store)

      assert conn.status == 200
      assert %{"Volumes" => vols, "Err" => ""} = decode(conn)
      names = vols |> Enum.map(& &1["Name"]) |> Enum.sort()
      assert names == ["vol-1", "vol-2"]
    end

    test "returns empty list when store is empty", %{store: store} do
      conn = post("/VolumeDriver.List", %{}, store: store)
      assert decode(conn) == %{"Volumes" => [], "Err" => ""}
    end
  end

  describe "POST /VolumeDriver.Path" do
    test "returns empty Mountpoint (Mount not yet implemented)", %{store: store} do
      conn = post("/VolumeDriver.Path", %{"Name" => "anything"}, store: store)
      assert conn.status == 200
      assert decode(conn) == %{"Mountpoint" => "", "Err" => ""}
    end
  end

  describe "Mount and Unmount placeholders" do
    test "Mount returns a non-empty Err until #310 lands", %{store: store} do
      conn = post("/VolumeDriver.Mount", %{"Name" => "any", "ID" => "abc"}, store: store)
      assert conn.status == 200
      assert %{"Err" => err} = decode(conn)
      assert err =~ "not implemented"
    end

    test "Unmount returns a non-empty Err until #310 lands", %{store: store} do
      conn = post("/VolumeDriver.Unmount", %{"Name" => "any", "ID" => "abc"}, store: store)
      assert conn.status == 200
      assert %{"Err" => err} = decode(conn)
      assert err =~ "not implemented"
    end
  end

  describe "unknown paths" do
    test "return 404 with Err", %{store: store} do
      conn = post("/bogus", %{}, store: store)
      assert conn.status == 404
      assert %{"Err" => "not found"} = decode(conn)
    end
  end
end
