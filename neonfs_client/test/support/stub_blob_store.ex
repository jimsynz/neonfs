defmodule NeonFS.Transport.StubBlobStore do
  @moduledoc false

  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @doc false
  def seed(hash, data, tier \\ :hot) do
    Agent.update(__MODULE__, &Map.put(&1, hash, {data, tier}))
  end

  def write_chunk(data, _volume_id, _tier, _opts \\ []) do
    hash = :crypto.hash(:sha256, data)
    Agent.update(__MODULE__, &Map.put(&1, hash, {data, :hot}))
    {:ok, hash, %{original_size: byte_size(data), stored_size: byte_size(data)}}
  end

  def read_chunk(hash, _volume_id, _opts \\ []) do
    case Agent.get(__MODULE__, &Map.get(&1, hash)) do
      {data, _tier} -> {:ok, data}
      nil -> {:error, "not found"}
    end
  end

  def chunk_info(hash) do
    case Agent.get(__MODULE__, &Map.get(&1, hash)) do
      {data, tier} -> {:ok, tier, byte_size(data)}
      nil -> {:error, :not_found}
    end
  end
end
