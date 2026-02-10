defmodule NeonFS.Core.StripeIndex do
  @moduledoc """
  GenServer managing stripe metadata with quorum-backed distributed storage.

  Provides fast lookups by stripe ID and queries by volume ID.
  Uses QuorumCoordinator for distributed writes/reads and maintains a local
  ETS cache for fast reads.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{MetadataCodec, QuorumCoordinator, Stripe}

  @stripe_key_prefix "stripe:"

  # Client API

  @doc """
  Starts the StripeIndex GenServer.

  ## Options

    * `:quorum_opts` — keyword list passed to QuorumCoordinator (must include `:ring`).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stores stripe metadata in ETS and writes to quorum store.

  Returns `{:ok, stripe_id}` on success.
  """
  @spec put(Stripe.t()) :: {:ok, binary()} | {:error, term()}
  def put(%Stripe{} = stripe) do
    GenServer.call(__MODULE__, {:put, stripe}, 10_000)
  end

  @doc """
  Retrieves stripe metadata by ID.

  First checks local ETS cache, then falls back to quorum read if not found locally.
  """
  @spec get(binary()) :: {:ok, Stripe.t()} | {:error, :not_found}
  def get(stripe_id) when is_binary(stripe_id) do
    case :ets.lookup(:stripe_index, stripe_id) do
      [{^stripe_id, stripe}] ->
        {:ok, stripe}

      [] ->
        get_from_quorum(stripe_id)
    end
  end

  @doc """
  Deletes stripe metadata from ETS and quorum store.
  """
  @spec delete(binary()) :: :ok | {:error, term()}
  def delete(stripe_id) when is_binary(stripe_id) do
    GenServer.call(__MODULE__, {:delete, stripe_id}, 10_000)
  end

  @doc """
  Checks whether stripe metadata exists for the given ID.

  Checks local ETS cache first, then falls back to quorum read.
  """
  @spec exists?(binary()) :: boolean()
  def exists?(stripe_id) when is_binary(stripe_id) do
    case :ets.lookup(:stripe_index, stripe_id) do
      [{^stripe_id, _}] -> true
      [] -> match?({:ok, _}, get_from_quorum(stripe_id))
    end
  end

  @doc """
  Returns all stripes for a given volume_id via local ETS scan.
  """
  @spec list_by_volume(binary()) :: [Stripe.t()]
  def list_by_volume(volume_id) when is_binary(volume_id) do
    :ets.foldl(
      fn
        {_id, %Stripe{volume_id: ^volume_id} = stripe}, acc ->
          [stripe | acc]

        _, acc ->
          acc
      end,
      [],
      :stripe_index
    )
  end

  @doc """
  Returns all stripes across all volumes from local ETS cache.
  """
  @spec list_all() :: [Stripe.t()]
  def list_all do
    :ets.foldl(
      fn
        {_id, %Stripe{} = stripe}, acc -> [stripe | acc]
        _, acc -> acc
      end,
      [],
      :stripe_index
    )
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    :ets.new(:stripe_index, [:set, :named_table, :public, read_concurrency: true])

    quorum_opts = Keyword.get(opts, :quorum_opts)
    :persistent_term.put({__MODULE__, :quorum_opts}, quorum_opts)

    case load_from_local_store() do
      {:ok, count} ->
        Logger.info("StripeIndex started, loaded #{count} stripes from local store")

      {:error, reason} ->
        Logger.debug("StripeIndex started, local store not available: #{inspect(reason)}")
    end

    {:ok, %{quorum_opts: quorum_opts}}
  end

  @impl true
  def handle_call({:put, stripe}, _from, state) do
    key = stripe_key(stripe.id)
    storable = stripe_to_storable_map(stripe)

    case QuorumCoordinator.quorum_write(key, storable, quorum_opts()) do
      {:ok, :written} ->
        :ets.insert(:stripe_index, {stripe.id, stripe})
        {:reply, {:ok, stripe.id}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:delete, stripe_id}, _from, state) do
    key = stripe_key(stripe_id)

    case QuorumCoordinator.quorum_delete(key, quorum_opts()) do
      {:ok, :written} ->
        :ets.delete(:stripe_index, stripe_id)
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :persistent_term.erase({__MODULE__, :quorum_opts})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # Private — Current quorum opts (always read from persistent_term to get
  # the latest ring after rebuild_quorum_ring updates it)

  defp quorum_opts do
    :persistent_term.get({__MODULE__, :quorum_opts}, nil)
  end

  # Private — Quorum reads

  defp get_from_quorum(stripe_id) do
    if opts = quorum_opts() do
      key = stripe_key(stripe_id)

      case QuorumCoordinator.quorum_read(key, opts) do
        {:ok, value} ->
          stripe = storable_map_to_stripe(value)
          :ets.insert(:stripe_index, {stripe_id, stripe})
          {:ok, stripe}

        {:ok, value, :possibly_stale} ->
          stripe = storable_map_to_stripe(value)
          :ets.insert(:stripe_index, {stripe_id, stripe})
          {:ok, stripe}

        {:error, :not_found} ->
          {:error, :not_found}

        {:error, _reason} ->
          {:error, :not_found}
      end
    else
      {:error, :not_found}
    end
  rescue
    _ -> {:error, :not_found}
  end

  # Private — Key format

  defp stripe_key(stripe_id) when is_binary(stripe_id) do
    @stripe_key_prefix <> stripe_id
  end

  # Private — Serialisation

  defp stripe_to_storable_map(%Stripe{} = stripe) do
    %{
      id: stripe.id,
      volume_id: stripe.volume_id,
      config: stripe.config,
      chunks: stripe.chunks,
      partial: stripe.partial,
      data_bytes: stripe.data_bytes,
      padded_bytes: stripe.padded_bytes
    }
  end

  defp storable_map_to_stripe(map) when is_map(map) do
    %Stripe{
      id: get_field(map, :id),
      volume_id: get_field(map, :volume_id),
      config: decode_config(get_field(map, :config, %{})),
      chunks: get_field(map, :chunks, []),
      partial: get_field(map, :partial, false),
      data_bytes: get_field(map, :data_bytes, 0),
      padded_bytes: get_field(map, :padded_bytes, 0)
    }
  end

  defp get_field(map, key, default \\ nil) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key)) || default
  end

  defp decode_config(config) when is_map(config) do
    %{
      data_chunks: get_field(config, :data_chunks),
      parity_chunks: get_field(config, :parity_chunks),
      chunk_size: get_field(config, :chunk_size)
    }
  end

  # Private — Local store loading

  defp load_from_local_store do
    drives = Application.get_env(:neonfs_core, :drives) || default_drives()

    count =
      Enum.reduce(drives, 0, fn drive, total ->
        drive_path = drive_path(drive)
        meta_dir = Path.join(drive_path, "meta")

        case File.ls(meta_dir) do
          {:ok, segment_dirs} ->
            total + load_segments_from_disk(meta_dir, segment_dirs)

          {:error, _} ->
            total
        end
      end)

    {:ok, count}
  rescue
    _ -> {:error, :not_available}
  end

  defp load_segments_from_disk(meta_dir, segment_dirs) do
    Enum.reduce(segment_dirs, 0, fn segment_hex, count ->
      segment_dir = Path.join(meta_dir, segment_hex)
      key_files = walk_metadata_files(segment_dir)
      count + load_stripe_files(key_files)
    end)
  end

  defp load_stripe_files(file_paths) do
    Enum.reduce(file_paths, 0, fn file_path, count ->
      case load_stripe_file(file_path) do
        :ok -> count + 1
        :skip -> count
      end
    end)
  end

  defp load_stripe_file(file_path) do
    with {:ok, data} <- File.read(file_path),
         {:ok, stripe} <- decode_stripe_record(data) do
      :ets.insert(:stripe_index, {stripe.id, stripe})
      :ok
    else
      _ -> :skip
    end
  end

  defp walk_metadata_files(dir) do
    case File.ls(dir) do
      {:ok, entries} ->
        Enum.flat_map(entries, &collect_metadata_entry(dir, &1))

      {:error, _} ->
        []
    end
  end

  defp collect_metadata_entry(dir, entry) do
    path = Path.join(dir, entry)

    cond do
      File.dir?(path) -> walk_metadata_files(path)
      String.contains?(entry, ".tmp") -> []
      true -> [path]
    end
  end

  defp decode_stripe_record(data) do
    case MetadataCodec.decode_record(data) do
      {:ok, %{tombstone: true}} ->
        :skip

      {:ok, %{value: value}} when is_map(value) ->
        if stripe_metadata?(value) do
          {:ok, storable_map_to_stripe(value)}
        else
          :skip
        end

      _ ->
        :skip
    end
  end

  defp stripe_metadata?(map) do
    (Map.has_key?(map, :id) or Map.has_key?(map, "id")) and
      (Map.has_key?(map, :volume_id) or Map.has_key?(map, "volume_id")) and
      (Map.has_key?(map, :config) or Map.has_key?(map, "config"))
  end

  defp default_drives do
    base_dir = Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs/blobs")
    [%{id: "default", path: base_dir, tier: :hot, capacity: 0}]
  end

  defp drive_path(%{path: path}), do: path
  defp drive_path(drive) when is_map(drive), do: Map.get(drive, :path, Map.get(drive, "path", ""))
end
