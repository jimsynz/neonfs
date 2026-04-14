defmodule NeonFS.Integration.WebDAVCoreBridge do
  @moduledoc false

  # Bridges WebDAV Backend `call_core` calls to real core nodes via RPC.
  #
  # Delegates directly to `NeonFS.Core` facade functions, which handle
  # volume name → ID resolution and dispatch to internal modules.
  #
  # Usage:
  #   WebDAVCoreBridge.store_core_node(node_atom)
  #   Application.put_env(:neonfs_webdav, :core_call_fn, &WebDAVCoreBridge.call/2)

  @spec store_core_node(node()) :: :ok
  def store_core_node(node_atom) do
    :persistent_term.put(:webdav_integration_core_node, node_atom)
    :ok
  end

  @spec cleanup :: :ok
  def cleanup do
    try do
      :persistent_term.erase(:webdav_integration_core_node)
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  @spec call(atom(), [term()]) :: term()
  def call(function, args) do
    core_node = :persistent_term.get(:webdav_integration_core_node)
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
