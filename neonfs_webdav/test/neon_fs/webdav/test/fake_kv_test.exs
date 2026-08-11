defmodule NeonFS.WebDAV.Test.FakeKVTest do
  use ExUnit.Case, async: false

  alias NeonFS.WebDAV.Test.FakeKV

  setup do
    FakeKV.stub!()

    on_exit(fn -> Application.delete_env(:neonfs_webdav, :kv_call_fn) end)

    :ok
  end

  test "the table is owned by a process that outlives the test resetting it" do
    owner = :ets.info(:ets.whereis(FakeKV), :owner)

    refute owner == self()
    assert Process.alive?(owner)
  end

  test "reset clears entries stored before it" do
    FakeKV.call(:put, ["webdav_lock:leftover", %{}])

    FakeKV.reset()

    assert FakeKV.call(:list_prefix, ["webdav_lock:"]) == []
  end
end
