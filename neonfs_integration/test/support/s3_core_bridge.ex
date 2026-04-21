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
    call(:write_file, [volume_name, path, body, opts])
  end

  def call(function, args) do
    core_node = :persistent_term.get(:s3_integration_core_node)
    rpc(core_node, NeonFS.Core, function, args)
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
