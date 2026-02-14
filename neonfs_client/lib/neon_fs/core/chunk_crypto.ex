defmodule NeonFS.Core.ChunkCrypto do
  @moduledoc """
  Per-chunk encryption metadata.

  Stores the minimum information needed to locate the correct key and decrypt
  a chunk: the algorithm used, the unique nonce, and which key version encrypted it.
  """

  alias __MODULE__

  @type t :: %__MODULE__{
          algorithm: :aes_256_gcm,
          nonce: binary(),
          key_version: pos_integer()
        }

  defstruct [:algorithm, :nonce, :key_version]

  @doc """
  Creates a new ChunkCrypto metadata entry.

  ## Options

  - `:algorithm` - Encryption algorithm (default: `:aes_256_gcm`)
  - `:nonce` - 12-byte unique nonce (required)
  - `:key_version` - Key version used for encryption (required)

  ## Examples

      iex> ChunkCrypto.new(nonce: :crypto.strong_rand_bytes(12), key_version: 1)
      %ChunkCrypto{algorithm: :aes_256_gcm, nonce: <<...>>, key_version: 1}
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    %ChunkCrypto{
      algorithm: Keyword.get(opts, :algorithm, :aes_256_gcm),
      nonce: Keyword.fetch!(opts, :nonce),
      key_version: Keyword.fetch!(opts, :key_version)
    }
  end

  @doc """
  Validates a ChunkCrypto metadata entry.

  Returns `:ok` if valid, or `{:error, reason}` if invalid.

  ## Validation Rules

  - Algorithm must be `:aes_256_gcm`
  - Nonce must be a 12-byte binary
  - Key version must be a positive integer
  """
  @spec validate(t()) :: :ok | {:error, String.t()}
  def validate(%ChunkCrypto{algorithm: alg}) when alg != :aes_256_gcm,
    do: {:error, "algorithm must be :aes_256_gcm, got: #{inspect(alg)}"}

  def validate(%ChunkCrypto{nonce: nonce}) when not is_binary(nonce) or byte_size(nonce) != 12,
    do: {:error, "nonce must be a 12-byte binary"}

  def validate(%ChunkCrypto{key_version: v}) when not is_integer(v) or v < 1,
    do: {:error, "key_version must be a positive integer"}

  def validate(%ChunkCrypto{}), do: :ok
end
