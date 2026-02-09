defmodule NeonFS.Core.StripeIndex do
  @moduledoc """
  GenServer managing stripe metadata with Ra-backed distributed storage.

  Provides fast lookups by stripe ID and queries by volume ID.
  Uses Ra consensus for writes and maintains a local ETS cache for fast reads.
  Follows the same architecture as ChunkIndex and FileIndex.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{RaServer, RaSupervisor, Stripe}

  # Client API

  @doc """
  Starts the StripeIndex GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stores stripe metadata in ETS and submits Ra command.

  Returns `{:ok, stripe_id}` on success.
  """
  @spec put(Stripe.t()) :: {:ok, binary()} | {:error, term()}
  def put(%Stripe{} = stripe) do
    GenServer.call(__MODULE__, {:put, stripe}, 10_000)
  end

  @doc """
  Retrieves stripe metadata by ID from local ETS cache.

  Returns `{:ok, stripe}` if found, `{:error, :not_found}` otherwise.
  """
  @spec get(binary()) :: {:ok, Stripe.t()} | {:error, :not_found}
  def get(stripe_id) when is_binary(stripe_id) do
    case :ets.lookup(:stripe_index, stripe_id) do
      [{^stripe_id, stripe}] ->
        {:ok, stripe}

      [] ->
        get_from_ra(stripe_id)
    end
  end

  @doc """
  Deletes stripe metadata from ETS and submits Ra command.
  """
  @spec delete(binary()) :: :ok | {:error, term()}
  def delete(stripe_id) when is_binary(stripe_id) do
    GenServer.call(__MODULE__, {:delete, stripe_id}, 10_000)
  end

  @doc """
  Returns all stripes for a given volume_id.
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
  Returns all stripes across all volumes.
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
  def init(_opts) do
    :ets.new(:stripe_index, [:set, :named_table, :public, read_concurrency: true])

    case restore_from_ra() do
      {:ok, count} ->
        Logger.info(
          "StripeIndex started with ETS table :stripe_index, restored #{count} stripes from Ra"
        )

      {:error, reason} ->
        Logger.debug("StripeIndex started but Ra not ready yet: #{inspect(reason)}")
    end

    {:ok, %{}}
  end

  @impl true
  def handle_call({:put, stripe}, _from, state) do
    stripe_map = struct_to_map(stripe)

    case maybe_ra_command({:put_stripe, stripe_map}) do
      {:ok, {:ok, _id}} ->
        :ets.insert(:stripe_index, {stripe.id, stripe})
        {:reply, {:ok, stripe.id}, state}

      {:error, :ra_not_available} ->
        :ets.insert(:stripe_index, {stripe.id, stripe})
        {:reply, {:ok, stripe.id}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:delete, stripe_id}, _from, state) do
    case maybe_ra_command({:delete_stripe, stripe_id}) do
      {:ok, :ok} ->
        :ets.delete(:stripe_index, stripe_id)
        {:reply, :ok, state}

      {:error, :ra_not_available} ->
        :ets.delete(:stripe_index, stripe_id)
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # Private Helpers

  defp get_from_ra(stripe_id) do
    case RaSupervisor.query(fn state ->
           stripes = Map.get(state, :stripes, %{})
           Map.get(stripes, stripe_id)
         end) do
      {:ok, nil} ->
        {:error, :not_found}

      {:ok, stripe_map} ->
        stripe = map_to_struct(stripe_map)
        :ets.insert(:stripe_index, {stripe_id, stripe})
        {:ok, stripe}

      {:error, _reason} ->
        {:error, :not_found}
    end
  catch
    :exit, _ -> {:error, :not_found}
  end

  # credo:disable-for-next-line Credo.Check.Refactor.Try
  defp maybe_ra_command(cmd) do
    initialized = RaServer.initialized?()

    try do
      case RaSupervisor.command(cmd) do
        {:ok, result, _leader} ->
          {:ok, result}

        {:error, :noproc} ->
          if initialized,
            do: {:error, :ra_unavailable},
            else: {:error, :ra_not_available}

        {:error, reason} ->
          {:error, reason}

        {:timeout, _node} ->
          {:error, :timeout}
      end
    catch
      :exit, {:noproc, _} ->
        if initialized,
          do: {:error, :ra_unavailable},
          else: {:error, :ra_not_available}

      kind, reason ->
        Logger.debug("Ra command error: #{inspect({kind, reason})}")

        if initialized,
          do: {:error, {:ra_error, {kind, reason}}},
          else: {:error, :ra_not_available}
    end
  end

  defp restore_from_ra do
    case RaSupervisor.query(fn state -> Map.get(state, :stripes, %{}) end) do
      {:ok, stripes} when is_map(stripes) ->
        count =
          Enum.reduce(stripes, 0, fn {id, stripe_map}, acc ->
            stripe = map_to_struct(stripe_map)
            :ets.insert(:stripe_index, {id, stripe})
            acc + 1
          end)

        {:ok, count}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp struct_to_map(%Stripe{} = stripe) do
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

  defp map_to_struct(stripe_map) when is_map(stripe_map) do
    %Stripe{
      id: stripe_map.id,
      volume_id: stripe_map.volume_id,
      config: stripe_map.config,
      chunks: Map.get(stripe_map, :chunks, []),
      partial: Map.get(stripe_map, :partial, false),
      data_bytes: Map.get(stripe_map, :data_bytes, 0),
      padded_bytes: Map.get(stripe_map, :padded_bytes, 0)
    }
  end
end
