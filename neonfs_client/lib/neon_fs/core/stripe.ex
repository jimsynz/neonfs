defmodule NeonFS.Core.Stripe do
  @moduledoc """
  Stripe struct for erasure-coded data.

  A stripe represents a group of data chunks plus their computed parity chunks,
  tracking the encoding configuration, chunk ordering, and partial-stripe metadata.
  Chunk ordering is significant: indices 0 through `data_chunks - 1` are data chunks,
  indices `data_chunks` through `data_chunks + parity_chunks - 1` are parity chunks.
  """

  alias __MODULE__

  @type config :: %{
          data_chunks: pos_integer(),
          parity_chunks: pos_integer(),
          chunk_size: pos_integer()
        }

  @type t :: %__MODULE__{
          id: binary(),
          volume_id: binary(),
          config: config(),
          chunks: [binary()],
          partial: boolean(),
          data_bytes: non_neg_integer(),
          padded_bytes: non_neg_integer()
        }

  defstruct [
    :id,
    :volume_id,
    :config,
    chunks: [],
    partial: false,
    data_bytes: 0,
    padded_bytes: 0
  ]

  @doc """
  Creates a new stripe with the given attributes.

  ## Parameters
    - `attrs` - Map or keyword list with stripe attributes

  ## Examples

      iex> Stripe.new(%{
      ...>   volume_id: "vol-1",
      ...>   config: %{data_chunks: 10, parity_chunks: 4, chunk_size: 262144}
      ...> })
      %Stripe{volume_id: "vol-1", ...}

  """
  @spec new(map() | keyword()) :: t()
  def new(attrs) when is_list(attrs), do: new(Map.new(attrs))

  def new(attrs) when is_map(attrs) do
    %Stripe{
      id: Map.get(attrs, :id, UUIDv7.generate()),
      volume_id: Map.get(attrs, :volume_id),
      config: Map.get(attrs, :config),
      chunks: Map.get(attrs, :chunks, []),
      partial: Map.get(attrs, :partial, false),
      data_bytes: Map.get(attrs, :data_bytes, 0),
      padded_bytes: Map.get(attrs, :padded_bytes, 0)
    }
  end

  @doc """
  Validates a stripe configuration.

  Returns `:ok` if valid, or `{:error, reason}` if invalid.
  """
  @spec validate(t()) :: :ok | {:error, String.t()}
  def validate(%Stripe{} = stripe) do
    with :ok <- validate_config(stripe.config),
         :ok <- validate_chunks(stripe.chunks, stripe.config) do
      validate_bytes(stripe.data_bytes, stripe.padded_bytes)
    end
  end

  @doc """
  Returns the total number of chunks (data + parity).
  """
  @spec total_chunks(t()) :: non_neg_integer()
  def total_chunks(%Stripe{config: %{data_chunks: dc, parity_chunks: pc}}) do
    dc + pc
  end

  @doc """
  Returns the data chunk hashes (first `data_chunks` entries).
  """
  @spec data_chunk_hashes(t()) :: [binary()]
  def data_chunk_hashes(%Stripe{chunks: chunks, config: %{data_chunks: dc}}) do
    Enum.take(chunks, dc)
  end

  @doc """
  Returns the parity chunk hashes (last `parity_chunks` entries).
  """
  @spec parity_chunk_hashes(t()) :: [binary()]
  def parity_chunk_hashes(%Stripe{chunks: chunks, config: %{data_chunks: dc}}) do
    Enum.drop(chunks, dc)
  end

  defp validate_config(%{data_chunks: dc, parity_chunks: pc, chunk_size: cs})
       when is_integer(dc) and dc >= 1 and is_integer(pc) and pc >= 1 and is_integer(cs) and
              cs > 0 do
    :ok
  end

  defp validate_config(_) do
    {:error, "invalid config: data_chunks >= 1, parity_chunks >= 1, chunk_size > 0"}
  end

  defp validate_chunks(chunks, config) when is_list(chunks) do
    expected = config.data_chunks + config.parity_chunks

    if chunks == [] or length(chunks) == expected do
      :ok
    else
      {:error, "chunks list length #{length(chunks)} does not match expected #{expected}"}
    end
  end

  defp validate_chunks(_, _), do: {:error, "chunks must be a list"}

  defp validate_bytes(data_bytes, padded_bytes)
       when is_integer(data_bytes) and data_bytes >= 0 and is_integer(padded_bytes) and
              padded_bytes >= 0 do
    :ok
  end

  defp validate_bytes(_, _) do
    {:error, "data_bytes and padded_bytes must be non-negative integers"}
  end
end
