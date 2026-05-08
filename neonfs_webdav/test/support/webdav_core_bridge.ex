defmodule NeonFS.WebDAV.IntegrationTest.CoreBridge do
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
  def call(:write_file_streamed, [volume_name, path, stream, opts]) do
    # Streams cannot cross `:rpc.call`: their closures often capture local
    # process state (e.g. WebDAV's `Plug.Conn` in the process dictionary).
    # Drain the stream here in the WebDAV handler process so the closure
    # has access to its captured state, then send the assembled binary
    # over to the remote core node via the batch write API.
    body = stream |> Enum.to_list() |> IO.iodata_to_binary()
    call(:write_file_at, [volume_name, path, 0, body, opts])
  end

  # Single-drive integration harness: pin replicate:1 unless the
  # caller already supplied opts. Default-durability volumes
  # (`replicate: factor=3, min_copies=2`) can't satisfy the gate's
  # `min_copies` against one drive, and once `Volume.MetadataWriter`
  # is wired into FileIndex (#835) lazy provisioning surfaces that
  # mismatch as a test failure rather than a silent skip.
  def call(:create_volume, [name]) do
    opts = [durability: %{type: :replicate, factor: 1, min_copies: 1}]
    call(:create_volume, [name, opts])
  end

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
