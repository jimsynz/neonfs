defmodule NeonFS.Client.ChunkReader do
  @moduledoc """
  Assembles file contents by fetching chunk references from core and reading
  the chunk bytes directly over the TLS data plane.

  Interface nodes (FUSE, NFS, S3, WebDAV) use this helper in place of
  `NeonFS.Core.read_file/3` to keep bulk data off the Erlang distribution
  control plane. The flow:

    1. `NeonFS.Client.Router.call/4` fetches `read_file_refs` metadata from
       a core node (metadata only, small payload).
    2. For each chunk ref, a location is selected and the chunk bytes are
       fetched via `Router.data_call(:get_chunk, ...)` over TLS.
    3. The byte range is sliced and assembled.

  Chunks that require server-side processing (decompression or decryption)
  cannot be read through the raw-bytes data plane — the bytes would arrive
  opaque. For those files this helper falls back to `read_file/3` for a
  correct result; the data plane optimisation applies to uncompressed,
  unencrypted volumes.

  Erasure-coded (stripe-based) files likewise fall back to `read_file/3`
  until streaming stripe reconstruction over the data plane is implemented.

  If every location for a chunk returns `:no_data_endpoint` (no TLS pool
  configured to that peer), the helper also falls back to `read_file/3`
  so that callers on nodes without a data-plane pool still get correct
  results. All other data-plane errors propagate.
  """

  require Logger

  alias NeonFS.Client.Router

  @default_chunk_timeout 30_000

  @type read_opts :: [
          offset: non_neg_integer(),
          length: non_neg_integer() | :all,
          timeout: timeout(),
          exclude_nodes: [node()]
        ]

  @doc """
  Reads a byte range from a file, fetching chunks over the data plane where
  possible and falling back to `read_file/3` when chunks require server-side
  processing or when erasure-coded.

  Options:

    * `:offset` - byte offset to start reading (default 0)
    * `:length` - number of bytes to read (default `:all`)
    * `:timeout` - per-chunk data-plane timeout in ms (default 30_000)
    * `:exclude_nodes` - nodes to skip when selecting a chunk location
      (useful for avoiding known-bad replicas)
  """
  @spec read_file(String.t(), String.t(), read_opts()) ::
          {:ok, binary()} | {:error, term()}
  def read_file(volume_name, path, opts \\ []) do
    refs_opts = Keyword.take(opts, [:offset, :length])

    case Router.call(NeonFS.Core, :read_file_refs, [volume_name, path, refs_opts]) do
      {:ok, %{chunks: chunks} = result} ->
        dispatch_read(chunks, result.file_size, volume_name, path, opts)

      {:error, :stripe_refs_unsupported} ->
        fallback_read(volume_name, path, opts)

      {:error, _} = error ->
        error
    end
  end

  defp dispatch_read(chunks, file_size, volume_name, path, opts) do
    if Enum.any?(chunks, &needs_server_processing?/1) do
      fallback_read(volume_name, path, opts)
    else
      case assemble(chunks, file_size, opts) do
        {:error, :no_data_endpoint} -> fallback_read(volume_name, path, opts)
        other -> other
      end
    end
  end

  defp assemble(chunks, _file_size, opts) do
    timeout = Keyword.get(opts, :timeout, @default_chunk_timeout)
    exclude = Keyword.get(opts, :exclude_nodes, [])

    chunks
    |> Enum.reduce_while({:ok, []}, fn ref, {:ok, acc} ->
      case fetch_chunk_bytes(ref, exclude, timeout) do
        {:ok, bytes} ->
          sliced = binary_part(bytes, ref.read_start, ref.read_length)
          {:cont, {:ok, [sliced | acc]}}

        {:error, _} = err ->
          {:halt, err}
      end
    end)
    |> case do
      {:ok, parts} -> {:ok, parts |> Enum.reverse() |> IO.iodata_to_binary()}
      error -> error
    end
  end

  defp fetch_chunk_bytes(ref, exclude, timeout) do
    ordered =
      ref.locations
      |> Enum.reject(&(&1.node in exclude))
      |> prefer_local()

    case ordered do
      [] ->
        {:error, :no_available_locations}

      locations ->
        try_locations(locations, ref, timeout, :no_locations_tried)
    end
  end

  defp try_locations([], _ref, _timeout, last_error), do: {:error, last_error}

  defp try_locations([loc | rest], ref, timeout, _last_error) do
    tier = tier_to_string(Map.get(loc, :tier, :hot))
    drive_id = Map.get(loc, :drive_id, "default")

    args = [hash: ref.hash, volume_id: drive_id, tier: tier]

    case Router.data_call(loc.node, :get_chunk, args, timeout: timeout) do
      {:ok, bytes} ->
        {:ok, bytes}

      {:error, reason} ->
        Logger.debug("Data-plane chunk fetch failed, trying next location",
          node: loc.node,
          reason: inspect(reason)
        )

        try_locations(rest, ref, timeout, reason)
    end
  end

  defp prefer_local(locations) do
    local = Node.self()
    {local_locs, remote_locs} = Enum.split_with(locations, &(&1.node == local))
    local_locs ++ Enum.shuffle(remote_locs)
  end

  defp tier_to_string(:hot), do: "hot"
  defp tier_to_string(:warm), do: "warm"
  defp tier_to_string(:cold), do: "cold"
  defp tier_to_string(tier) when is_binary(tier), do: tier

  defp needs_server_processing?(%{compression: compression, encrypted: encrypted}) do
    compression != :none or encrypted
  end

  defp fallback_read(volume_name, path, opts) do
    forward_opts = Keyword.take(opts, [:offset, :length])
    Router.call(NeonFS.Core, :read_file, [volume_name, path, forward_opts])
  end
end
