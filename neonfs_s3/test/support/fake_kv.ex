defmodule NeonFS.S3.Test.FakeKV do
  @moduledoc """
  ETS-backed stand-in for `NeonFS.Client.KV` in unit tests, so the
  KV-backed `MultipartStore` (#1177) runs without a cluster. Call
  `stub!/0` in setup after `use Mimic` — it clears the table and stubs
  the four KV functions the store uses.
  """

  @table __MODULE__

  @spec stub!() :: :ok
  def stub! do
    reset()
    Mimic.stub(NeonFS.Client.KV, :put, &__MODULE__.put/2)
    Mimic.stub(NeonFS.Client.KV, :get, &__MODULE__.get/1)
    Mimic.stub(NeonFS.Client.KV, :delete, &__MODULE__.delete/1)
    Mimic.stub(NeonFS.Client.KV, :list_prefix, &__MODULE__.list_prefix/1)
    :ok
  end

  @spec reset() :: :ok
  def reset do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:named_table, :public, :set])
    else
      :ets.delete_all_objects(@table)
    end

    :ok
  end

  @spec put(binary(), term()) :: :ok
  def put(key, value) when is_binary(key) do
    :ets.insert(@table, {key, value})
    :ok
  end

  @spec get(binary()) :: {:ok, term()} | {:error, :not_found}
  def get(key) when is_binary(key) do
    case :ets.lookup(@table, key) do
      [{^key, value}] -> {:ok, value}
      [] -> {:error, :not_found}
    end
  end

  @spec delete(binary()) :: :ok
  def delete(key) when is_binary(key) do
    :ets.delete(@table, key)
    :ok
  end

  @spec list_prefix(binary()) :: [{binary(), term()}]
  def list_prefix(prefix) when is_binary(prefix) do
    @table
    |> :ets.tab2list()
    |> Enum.filter(fn {key, _value} -> String.starts_with?(key, prefix) end)
  end
end
