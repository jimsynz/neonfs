defmodule NeonFS.Integration.S3CoreBridge do
  @moduledoc false

  # Bridges S3 Backend `call_core` calls to real core nodes via RPC.
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
  def call(function, args) do
    core_node = :persistent_term.get(:s3_integration_core_node)
    do_call(core_node, function, args)
  end

  defp do_call(node, :lookup_s3_credential, [access_key_id]) do
    rpc(node, NeonFS.Core, :lookup_s3_credential, [access_key_id])
  end

  defp do_call(node, :list_volumes, []) do
    case rpc(node, NeonFS.Core.VolumeRegistry, :list, []) do
      volumes when is_list(volumes) -> {:ok, volumes}
      other -> other
    end
  end

  defp do_call(node, :create_volume, [name]) do
    rpc(node, NeonFS.Core.VolumeRegistry, :create, [name, []])
  end

  defp do_call(node, :create_volume, [name, _opts]) do
    rpc(node, NeonFS.Core.VolumeRegistry, :create, [name, []])
  end

  defp do_call(node, :delete_volume, [name]) do
    with {:ok, volume} <- rpc(node, NeonFS.Core.VolumeRegistry, :get_by_name, [name]) do
      rpc(node, NeonFS.Core.VolumeRegistry, :delete, [volume.id])
    end
  end

  defp do_call(node, :get_volume, [name]) do
    rpc(node, NeonFS.Core.VolumeRegistry, :get_by_name, [name])
  end

  defp do_call(node, :volume_exists?, [name]) do
    case rpc(node, NeonFS.Core.VolumeRegistry, :get_by_name, [name]) do
      {:ok, _} -> true
      _ -> false
    end
  end

  defp do_call(node, :write_file, [volume_name, key, content | _opts]) do
    rpc(node, NeonFS.TestHelpers, :write_file, [volume_name, ensure_leading_slash(key), content])
  end

  defp do_call(node, :read_file, [volume_name, key]) do
    rpc(node, NeonFS.TestHelpers, :read_file, [volume_name, ensure_leading_slash(key)])
  end

  defp do_call(node, :delete_file, [volume_name, key]) do
    rpc(node, NeonFS.TestHelpers, :delete_file, [volume_name, ensure_leading_slash(key)])
  end

  defp do_call(node, :get_file_meta, [volume_name, key]) do
    rpc(node, NeonFS.TestHelpers, :get_file, [volume_name, ensure_leading_slash(key)])
  end

  defp do_call(node, :list_files, [volume_name, path]) do
    case rpc(node, NeonFS.TestHelpers, :list_files, [volume_name]) do
      {:ok, files} ->
        filtered = filter_direct_children(files, normalise_path(path))
        {:ok, filtered}

      {:error, _} = error ->
        error
    end
  end

  defp do_call(_node, function, args) do
    {:error, {:not_implemented, function, args}}
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

  defp filter_direct_children(files, parent_path) do
    Enum.filter(files, fn file ->
      direct_child?(parent_path, file.path)
    end)
  end

  defp direct_child?("/", file_path) do
    trimmed = String.trim_leading(file_path, "/")
    trimmed != "" and not String.contains?(trimmed, "/")
  end

  defp direct_child?(parent, file_path) do
    parent_with_slash = if String.ends_with?(parent, "/"), do: parent, else: parent <> "/"

    if String.starts_with?(file_path, parent_with_slash) do
      rest = String.trim_leading(file_path, parent_with_slash)
      rest != "" and not String.contains?(rest, "/")
    else
      false
    end
  end

  defp ensure_leading_slash("/" <> _ = path), do: path
  defp ensure_leading_slash(path), do: "/" <> path

  defp normalise_path("/"), do: "/"

  defp normalise_path(path) do
    path
    |> String.trim_trailing("/")
    |> then(fn
      "/" <> _ = p -> p
      p -> "/" <> p
    end)
  end
end
