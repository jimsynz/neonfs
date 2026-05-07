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
  alias NeonFS.Core.Volume.MetadataReader

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
  Retrieves stripe metadata by `volume_id` and `stripe_id`.

  Resolves through `Volume.MetadataReader.get_stripe/3`. The local ETS
  table is a write-through materialisation for list operations on this
  node — serving point reads from it would return stale values for
  keys written or deleted elsewhere in the cluster (#342).
  """
  @spec get(binary(), binary()) :: {:ok, Stripe.t()} | {:error, :not_found}
  def get(volume_id, stripe_id) when is_binary(volume_id) and is_binary(stripe_id) do
    get_from_metadata_reader(volume_id, stripe_id)
  end

  @doc """
  Legacy stripe_id-only lookup. Retained for callers that do not yet
  have a `volume_id` on hand. Callers with volume context should
  prefer `get/2`. This entry point will be retired once #837's
  caller-threading is complete.
  """
  @spec get(binary()) :: {:ok, Stripe.t()} | {:error, :not_found}
  def get(stripe_id) when is_binary(stripe_id) do
    get_from_quorum(stripe_id)
  end

  @doc """
  Deletes stripe metadata from ETS and quorum store.
  """
  @spec delete(binary()) :: :ok | {:error, term()}
  def delete(stripe_id) when is_binary(stripe_id) do
    GenServer.call(__MODULE__, {:delete, stripe_id}, 10_000)
  end

  @doc """
  Checks whether stripe metadata exists for the given `volume_id` /
  `stripe_id`.

  Resolves through `Volume.MetadataReader.get_stripe/3` — see `get/2`
  for rationale.
  """
  @spec exists?(binary(), binary()) :: boolean()
  def exists?(volume_id, stripe_id) when is_binary(volume_id) and is_binary(stripe_id) do
    match?({:ok, _}, get_from_metadata_reader(volume_id, stripe_id))
  end

  @doc """
  Legacy stripe_id-only existence check. Same caveat as `get/1`.
  """
  @spec exists?(binary()) :: boolean()
  def exists?(stripe_id) when is_binary(stripe_id) do
    match?({:ok, _}, get_from_quorum(stripe_id))
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

    # Use explicit opts first (unit tests pass quorum_opts directly).
    # Fall back to persistent_term (set by Supervisor or rebuild_quorum_ring).
    # On crash restart, child_spec has no quorum_opts, so persistent_term
    # (preserved by crash-safe terminate) provides the authoritative ring.
    quorum_opts =
      Keyword.get(opts, :quorum_opts) ||
        :persistent_term.get({__MODULE__, :quorum_opts}, nil)

    :persistent_term.put({__MODULE__, :quorum_opts}, quorum_opts)

    metadata_reader_opts =
      Keyword.get(opts, :metadata_reader_opts) ||
        :persistent_term.get({__MODULE__, :metadata_reader_opts}, [])

    :persistent_term.put({__MODULE__, :metadata_reader_opts}, metadata_reader_opts)

    case load_from_local_store() do
      {:ok, count} ->
        Logger.info("StripeIndex started, loaded stripes from local store", count: count)

      {:error, reason} ->
        Logger.debug("StripeIndex started, local store not available", reason: reason)
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

  # Only erase persistent_term on clean shutdown, not on crash. On crash
  # restart, the surviving persistent_term value (set by rebuild_quorum_ring)
  # prevents the child from overwriting it with a stale child_spec ring.
  @impl true
  def terminate(reason, _state) when reason in [:normal, :shutdown] do
    safe_erase_quorum_opts()
  end

  def terminate({:shutdown, _}, _state) do
    safe_erase_quorum_opts()
  end

  def terminate(_reason, _state), do: :ok

  defp safe_erase_quorum_opts do
    :persistent_term.erase({__MODULE__, :quorum_opts})

    try do
      :persistent_term.erase({__MODULE__, :metadata_reader_opts})
    rescue
      ArgumentError -> :ok
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  # Private — Current quorum opts (always read from persistent_term to get
  # the latest ring after rebuild_quorum_ring updates it)

  defp quorum_opts do
    :persistent_term.get({__MODULE__, :quorum_opts}, nil)
  end

  defp metadata_reader_opts do
    :persistent_term.get({__MODULE__, :metadata_reader_opts}, [])
  end

  # Private — MetadataReader-backed read with QuorumCoordinator fallback.
  # The fallback is load-bearing today because the per-volume index tree
  # is only populated once the write-side migration (#787) lands — until
  # then, `MetadataReader` returns `:not_found` in production for every
  # key, and the only real source of truth is the old quorum-replicated
  # `<drive>/meta/` segments. Once #787 lands the fallback can go.

  defp get_from_metadata_reader(volume_id, stripe_id) do
    key = stripe_key(stripe_id)

    case MetadataReader.get_stripe(volume_id, key, metadata_reader_opts()) do
      {:ok, value} ->
        stripe = storable_map_to_stripe(value)
        :ets.insert(:stripe_index, {stripe_id, stripe})
        {:ok, stripe}

      _ ->
        get_from_quorum(stripe_id)
    end
  rescue
    _ -> get_from_quorum(stripe_id)
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
