defmodule NeonFS.WebDAV.Test.FakeKV do
  @moduledoc """
  ETS-backed stand-in for `NeonFS.Client.KV` in unit tests, so the
  KV-backed `LockStore` runs without a cluster. Call `stub!/0`
  in setup — it resets the table (creating it under a suite-lifetime
  owner on first use) and installs itself via the `:kv_call_fn`
  injection point, matching this package's `*_call_fn` convention.

  Mirrors `NeonFS.S3.Test.FakeKV`; kept per-package until a
  third consumer justifies promoting a shared fake into
  `neonfs_test_support`.

  Callers are responsible for `Application.delete_env(:neonfs_webdav,
  :kv_call_fn)` in their own `on_exit` — this module avoids ExUnit
  calls so dialyzer can analyse it without ExUnit in the PLT.
  """

  @table __MODULE__

  @spec stub!() :: :ok
  def stub! do
    reset()
    Application.put_env(:neonfs_webdav, :kv_call_fn, &call/2)
    :ok
  end

  @spec reset() :: :ok
  def reset do
    ensure_owner()
    :ets.delete_all_objects(@table)
    :ok
  end

  @spec call(atom(), [term()]) :: term()
  def call(:put, [key, value]) do
    :ets.insert(@table, {key, value})
    :ok
  end

  def call(:get, [key]) do
    case :ets.lookup(@table, key) do
      [{^key, value}] -> {:ok, value}
      [] -> {:error, :not_found}
    end
  end

  def call(:delete, [key]) do
    :ets.delete(@table, key)
    :ok
  end

  def call(:list_prefix, [prefix]) do
    @table
    |> :ets.tab2list()
    |> Enum.filter(fn {key, _value} -> String.starts_with?(key, prefix) end)
  end

  # The table must outlive any single test. If the test process that
  # first called `reset/0` owned it, the table would die when that
  # process exits — and a serial test whose `reset/0` ran while the
  # previous owner was still terminating would see the table vanish
  # mid-test. Park ownership in an unlinked Agent that lives for the
  # whole suite instead.
  defp ensure_owner do
    if :ets.whereis(@table) == :undefined do
      {:ok, _pid} = Agent.start(fn -> :ets.new(@table, [:named_table, :public, :set]) end)
    end

    :ok
  end
end
