defmodule NeonFS.Containerd.Digest do
  @moduledoc """
  Helpers for translating containerd digest strings to NeonFS volume
  paths.

  Layout decision (#547): sharded `sha256/<ab>/<cd>/<rest>`. Matches
  containerd local-store convention, friendly to large blob counts,
  avoids enumerating a flat directory with millions of entries.

  Only `sha256` is supported for now — containerd accepts other
  algorithms but every layer / manifest hash in OCI images is sha256
  in practice. Callers that need to plumb additional algorithms can
  extend this module rather than open-coding the layout.
  """

  @typedoc """
  Containerd digest in `<algorithm>:<hex>` form, e.g.
  `sha256:abc123...`.
  """
  @type digest :: String.t()

  @doc """
  Resolves a containerd digest to a sharded volume path.

  Returns `{:ok, path}` for a valid sha256 digest, `{:error, reason}`
  for malformed input or unsupported algorithms.

  ## Examples

      iex> NeonFS.Containerd.Digest.to_path("sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
      {:ok, "sha256/ab/cd/ef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}

      iex> NeonFS.Containerd.Digest.to_path("sha256:tooshort")
      {:error, :invalid_digest}

      iex> NeonFS.Containerd.Digest.to_path("md5:abc")
      {:error, :unsupported_algorithm}

      iex> NeonFS.Containerd.Digest.to_path("nope")
      {:error, :invalid_digest}
  """
  @spec to_path(digest()) ::
          {:ok, String.t()} | {:error, :invalid_digest | :unsupported_algorithm}
  def to_path(<<"sha256:", hex::binary>>) when byte_size(hex) == 64 do
    if valid_hex?(hex) do
      <<a::binary-size(2), b::binary-size(2), rest::binary>> = hex
      {:ok, "sha256/" <> a <> "/" <> b <> "/" <> rest}
    else
      {:error, :invalid_digest}
    end
  end

  def to_path(<<algo::binary-size(6), ":", _rest::binary>>) when algo != "sha256" do
    {:error, :unsupported_algorithm}
  end

  def to_path(other) when is_binary(other) do
    case String.split(other, ":", parts: 2) do
      [algo, _hex] when algo != "sha256" -> {:error, :unsupported_algorithm}
      _ -> {:error, :invalid_digest}
    end
  end

  defp valid_hex?(hex) do
    Regex.match?(~r/\A[0-9a-f]+\z/, hex)
  end
end
