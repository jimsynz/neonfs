defmodule NeonFS.Integration.S3CoreBridge do
  @moduledoc false

  # Bridges S3 Backend `call_core` calls to real core nodes via RPC.
  #
  # Now delegates directly to `NeonFS.Core` facade functions, which handle
  # volume name → ID resolution and dispatch to internal modules.
  #
  # Credential lookups are routed to NeonFS.Core.S3CredentialManager on the
  # core node. Call `create_test_credential/1` during setup to provision a
  # credential for the test run.
  #
  # Usage:
  #   S3CoreBridge.store_core_node(node_atom)
  #   {access_key, secret_key} = S3CoreBridge.create_test_credential(node_atom)
  #   Application.put_env(:neonfs_s3, :core_call_fn, &S3CoreBridge.call/2)

  @spec store_core_node(node()) :: :ok
  def store_core_node(node_atom) do
    :persistent_term.put(:s3_integration_core_node, node_atom)
    :ok
  end

  @spec create_test_credential(node()) :: {String.t(), String.t()}
  def create_test_credential(node_atom) do
    {:ok, credential} =
      rpc(node_atom, NeonFS.Core.S3CredentialManager, :create, [
        %{user: "integration-test"}
      ])

    access_key = credential.access_key_id
    secret_key = credential.secret_access_key

    :persistent_term.put(:s3_integration_test_access_key, access_key)
    :persistent_term.put(:s3_integration_test_secret_key, secret_key)

    {access_key, secret_key}
  end

  @spec test_access_key() :: String.t()
  def test_access_key, do: :persistent_term.get(:s3_integration_test_access_key)

  @spec test_secret_key() :: String.t()
  def test_secret_key, do: :persistent_term.get(:s3_integration_test_secret_key)

  @spec cleanup :: :ok
  def cleanup do
    :persistent_term.erase(:s3_integration_core_node)

    for key <- [:s3_integration_test_access_key, :s3_integration_test_secret_key] do
      try do
        :persistent_term.erase(key)
      rescue
        ArgumentError -> :ok
      end
    end

    case :ets.whereis(:s3_integration_chunk_stash) do
      :undefined -> :ok
      _ref -> :ets.delete(:s3_integration_chunk_stash)
    end

    :ok
  end

  @spec call(atom(), [term()]) :: term()
  def call(:write_file_streamed, [volume_name, path, stream, opts]) do
    # Streams cannot cross `:rpc.call`: their closures often capture local
    # process state (e.g. the S3 plug's `Plug.Conn` in the process
    # dictionary). Drain the stream here in the S3 handler process so the
    # closure has access to its captured state, then send the assembled
    # binary over to the remote core node via the batch write API.
    body = stream |> Enum.to_list() |> IO.iodata_to_binary()
    call(:write_file_at, [volume_name, path, 0, body, opts])
  end

  def call(function, args) do
    core_node = :persistent_term.get(:s3_integration_core_node)
    rpc(core_node, NeonFS.Core, function, args)
  end

  # `:ship_chunks_fn` hook for the S3 integration harness (#488).
  # The test runner has no TLS pool to the peer core nodes so the
  # real `ChunkWriter.write_file_stream/4` can't ship chunks. Drain
  # the body into a single synthetic ref, stash the bytes in an ETS
  # table keyed by sha256, return the ref to the caller. Paired with
  # `commit_refs/4`.
  @spec ship_chunks(String.t(), String.t(), term()) :: {:ok, [map()]}
  def ship_chunks(_bucket, _key, body) do
    body_binary = body_to_binary(body)
    hash = :crypto.hash(:sha256, body_binary)
    ensure_stash_table()
    :ets.insert(:s3_integration_chunk_stash, {hash, body_binary})

    ref = %{
      hash: hash,
      locations: [%{node: node(), drive_id: "default", tier: :hot}],
      size: byte_size(body_binary),
      codec: %{compression: :none, crypto: nil, original_size: byte_size(body_binary)}
    }

    {:ok, [ref]}
  end

  # `:commit_refs_fn` hook. Pull each ref's stashed bytes out of ETS,
  # concatenate in list order, and write the single assembled file on
  # the remote core via `write_file_at`. Mirrors what production's
  # `commit_chunks/4` RPC does — materialise a `FileIndex` entry —
  # but against the bridge so the integration harness doesn't need
  # TLS pools.
  @spec commit_refs(String.t(), String.t(), [map()], keyword()) :: term()
  def commit_refs(bucket, key, refs, write_opts) do
    combined =
      refs
      |> Enum.map(fn ref ->
        case :ets.lookup(:s3_integration_chunk_stash, ref.hash) do
          [{_, bytes}] -> bytes
          [] -> raise "unknown stashed chunk #{inspect(ref.hash)}"
        end
      end)
      |> IO.iodata_to_binary()

    call(:write_file_at, [bucket, key, 0, combined, write_opts])
  end

  defp body_to_binary(body) when is_binary(body), do: body
  defp body_to_binary(body) when is_list(body), do: IO.iodata_to_binary(body)
  defp body_to_binary(stream), do: stream |> Enum.to_list() |> IO.iodata_to_binary()

  defp ensure_stash_table do
    case :ets.whereis(:s3_integration_chunk_stash) do
      :undefined ->
        :ets.new(:s3_integration_chunk_stash, [:named_table, :public, :set])

      _ref ->
        :ok
    end
  end

  defp rpc(node, module, function, args) do
    case :rpc.call(node, module, function, args, 30_000) do
      {:badrpc, reason} -> {:error, {:badrpc, reason}}
      {:error, reason} -> {:error, normalise_error(reason)}
      result -> result
    end
  end

  defp normalise_error(%{class: :invalid} = err) do
    if err.message =~ "already exists", do: :already_exists, else: :invalid
  end

  defp normalise_error(%{class: :not_found}), do: :not_found
  defp normalise_error(:not_found), do: :not_found
  defp normalise_error(:already_exists), do: :already_exists
  defp normalise_error(other), do: other
end
