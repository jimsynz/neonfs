defmodule NeonFS.Core.StripeIndex do
  @moduledoc """
  GenServer managing stripe metadata.

  Reads delegate to `Volume.MetadataReader.get_stripe/3` and writes
  delegate to `Volume.MetadataWriter.put/5` / `delete/4`. The local
  ETS table is a write-through materialisation for `list_by_volume/1`
  and `list_all/0`, not the source of truth for point reads.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Stripe
  alias NeonFS.Core.Volume.{MetadataReader, MetadataValue, MetadataWriter}

  @stripe_key_prefix "stripe:"

  # Client API

  @doc """
  Starts the StripeIndex GenServer.

  ## Options

    * `:metadata_reader_opts` — keyword list forwarded to
      `Volume.MetadataReader.get_stripe/3`. Production callers
      typically rely on the persistent_term-backed default; tests
      pass an explicit stub.
    * `:metadata_writer_opts` — keyword list forwarded to
      `Volume.MetadataWriter.put/5` and `delete/4`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stores stripe metadata in ETS and writes to the per-volume index tree.

  Returns `{:ok, stripe_id}` on success.
  """
  @spec put(Stripe.t()) :: {:ok, binary()} | {:error, term()} | {:error, term(), map()}
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
  Deletes stripe metadata from ETS and the per-volume index tree.
  """
  @spec delete(binary()) :: :ok | {:error, term()} | {:error, term(), map()}
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

    metadata_reader_opts =
      Keyword.get(opts, :metadata_reader_opts) ||
        :persistent_term.get({__MODULE__, :metadata_reader_opts}, [])

    :persistent_term.put({__MODULE__, :metadata_reader_opts}, metadata_reader_opts)

    metadata_writer_opts =
      Keyword.get(opts, :metadata_writer_opts) ||
        :persistent_term.get({__MODULE__, :metadata_writer_opts}, [])

    :persistent_term.put({__MODULE__, :metadata_writer_opts}, metadata_writer_opts)

    {:ok, %{}}
  end

  @impl true
  def handle_call({:put, stripe}, _from, state) do
    case write_stripe(stripe) do
      :ok ->
        :ets.insert(:stripe_index, {stripe.id, stripe})
        {:reply, {:ok, stripe.id}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {:error, reason, info} ->
        {:reply, {:error, reason, info}, state}
    end
  end

  @impl true
  def handle_call({:delete, stripe_id}, _from, state) do
    case :ets.lookup(:stripe_index, stripe_id) do
      [{^stripe_id, %Stripe{volume_id: volume_id} = _stripe}] when is_binary(volume_id) ->
        case delete_stripe_meta(volume_id, stripe_id) do
          :ok ->
            :ets.delete(:stripe_index, stripe_id)
            {:reply, :ok, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}

          {:error, reason, info} ->
            {:reply, {:error, reason, info}, state}
        end

      _ ->
        # No local entry, nothing to delete in the index tree either —
        # the put would have populated both. Treat as idempotent :ok.
        :ets.delete(:stripe_index, stripe_id)
        {:reply, :ok, state}
    end
  end

  # Only erase persistent_term on clean shutdown, not on crash. On
  # crash restart, the surviving persistent_term value (set by the
  # previous incarnation's init) keeps reads/writes routed correctly.
  @impl true
  def terminate(reason, _state) when reason in [:normal, :shutdown] do
    safe_erase_persistent_terms()
  end

  def terminate({:shutdown, _}, _state) do
    safe_erase_persistent_terms()
  end

  def terminate(_reason, _state), do: :ok

  defp safe_erase_persistent_terms do
    for key <- [:metadata_reader_opts, :metadata_writer_opts] do
      try do
        :persistent_term.erase({__MODULE__, key})
      rescue
        ArgumentError -> :ok
      end
    end

    :ok
  end

  defp metadata_reader_opts do
    :persistent_term.get({__MODULE__, :metadata_reader_opts}, [])
  end

  defp metadata_writer_opts do
    :persistent_term.get({__MODULE__, :metadata_writer_opts}, [])
  end

  # Private — MetadataWriter operations

  defp write_stripe(%Stripe{} = stripe) do
    if is_binary(stripe.volume_id) do
      key = stripe_key(stripe.id)
      encoded = MetadataValue.encode(stripe_to_storable_map(stripe))

      case MetadataWriter.put(
             stripe.volume_id,
             :stripe_index,
             key,
             encoded,
             metadata_writer_opts()
           ) do
        {:ok, _root} -> :ok
        {:error, _, _} = err -> err
        {:error, _reason} = err -> err
      end
    else
      {:error, :missing_volume_id}
    end
  end

  defp delete_stripe_meta(volume_id, stripe_id) when is_binary(volume_id) do
    key = stripe_key(stripe_id)

    case MetadataWriter.delete(volume_id, :stripe_index, key, metadata_writer_opts()) do
      {:ok, _root} -> :ok
      {:error, _, _} = err -> err
      {:error, _reason} = err -> err
    end
  end

  defp get_from_metadata_reader(volume_id, stripe_id) do
    key = stripe_key(stripe_id)

    case MetadataReader.get_stripe(volume_id, key, metadata_reader_opts()) do
      {:ok, value} ->
        stripe = storable_map_to_stripe(value)
        :ets.insert(:stripe_index, {stripe_id, stripe})
        {:ok, stripe}

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, _reason} ->
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
end
