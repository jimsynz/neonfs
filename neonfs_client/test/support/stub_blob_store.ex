defmodule NeonFS.Transport.StubBlobStore do
  @moduledoc false

  use Agent

  @chunks_key :__chunks__
  @volume_opts_key :__volume_opts__
  @last_opts_key :__last_write_opts__

  def start_link(_opts \\ []) do
    Agent.start_link(
      fn -> %{@chunks_key => %{}, @volume_opts_key => %{}, @last_opts_key => []} end,
      name: __MODULE__
    )
  end

  @doc false
  def seed(hash, data, tier \\ :hot) do
    Agent.update(__MODULE__, fn state ->
      Map.update!(state, @chunks_key, &Map.put(&1, hash, {data, tier}))
    end)
  end

  @doc false
  def put_volume_opts(volume_id, opts) do
    Agent.update(__MODULE__, fn state ->
      Map.update!(state, @volume_opts_key, &Map.put(&1, volume_id, opts))
    end)
  end

  @doc false
  def last_write_opts do
    Agent.get(__MODULE__, &Map.get(&1, @last_opts_key, []))
  end

  def write_chunk(data, _drive_id, _tier, opts \\ []) do
    hash = :crypto.hash(:sha256, data)

    Agent.update(__MODULE__, fn state ->
      state
      |> Map.update!(@chunks_key, &Map.put(&1, hash, {data, :hot}))
      |> Map.put(@last_opts_key, opts)
    end)

    {:ok, hash, %{original_size: byte_size(data), stored_size: byte_size(data)}}
  end

  def read_chunk(hash, _drive_id, _opts \\ []) do
    case Agent.get(__MODULE__, &Map.get(&1, @chunks_key, %{})) |> Map.get(hash) do
      {data, _tier} -> {:ok, data}
      nil -> {:error, "not found"}
    end
  end

  def chunk_info(hash) do
    case Agent.get(__MODULE__, &Map.get(&1, @chunks_key, %{})) |> Map.get(hash) do
      {data, tier} -> {:ok, tier, byte_size(data)}
      nil -> {:error, :not_found}
    end
  end

  def resolve_put_chunk_opts(volume_id) when is_binary(volume_id) do
    Agent.get(__MODULE__, &Map.get(&1, @volume_opts_key, %{})) |> Map.get(volume_id, [])
  end
end
