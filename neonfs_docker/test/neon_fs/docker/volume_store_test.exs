defmodule NeonFS.Docker.VolumeStoreTest do
  use ExUnit.Case, async: true

  alias NeonFS.Docker.VolumeStore

  setup do
    name = :"store_#{System.unique_integer([:positive])}"
    {:ok, pid} = VolumeStore.start_link(name: name)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    {:ok, store: name}
  end

  test "put/3 stores a record and get/2 returns it", %{store: store} do
    assert :ok = VolumeStore.put(store, "vol-a", %{"k" => "v"})
    assert {:ok, %{name: "vol-a", opts: %{"k" => "v"}}} = VolumeStore.get(store, "vol-a")
  end

  test "put/3 is idempotent and overwrites the stored opts", %{store: store} do
    :ok = VolumeStore.put(store, "vol-a", %{"v" => "1"})
    :ok = VolumeStore.put(store, "vol-a", %{"v" => "2"})
    assert {:ok, %{opts: %{"v" => "2"}}} = VolumeStore.get(store, "vol-a")
  end

  test "get/2 returns not_found for unknown names", %{store: store} do
    assert {:error, :not_found} = VolumeStore.get(store, "missing")
  end

  test "delete/2 removes the record and is idempotent", %{store: store} do
    :ok = VolumeStore.put(store, "vol-a", %{})
    assert :ok = VolumeStore.delete(store, "vol-a")
    assert {:error, :not_found} = VolumeStore.get(store, "vol-a")
    assert :ok = VolumeStore.delete(store, "vol-a")
  end

  test "list/1 returns all records", %{store: store} do
    :ok = VolumeStore.put(store, "a", %{})
    :ok = VolumeStore.put(store, "b", %{})

    names =
      store
      |> VolumeStore.list()
      |> Enum.map(& &1.name)
      |> Enum.sort()

    assert names == ["a", "b"]
  end
end
