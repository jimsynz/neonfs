defmodule NeonFS.NFS.Native do
  @moduledoc """
  Native Rust NIF bindings for NFSv3 server operations.

  This module provides low-level bindings to the neonfs_nfs Rust crate,
  which handles NFSv3 protocol operations via the nfs3_server library.
  """

  use Rustler,
    otp_app: :neonfs_nfs,
    crate: :neonfs_nfs

  @type nfs_server :: reference()
  @type request_id :: non_neg_integer()

  @doc """
  Start an NFS server that communicates with the given callback PID.

  The server listens on the specified bind address (e.g. "0.0.0.0:2049").
  Operations will be sent as messages to the callback process in the format:
  `{:nfs_op, request_id, {operation_name, params}}`

  Returns `{:ok, server}` where server is an opaque reference.
  """
  @spec start_nfs_server(String.t(), pid()) :: {:ok, nfs_server()} | {:error, String.t()}
  def start_nfs_server(_bind_address, _callback_pid), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Stop a running NFS server.

  Signals the server to shut down gracefully, closing the TCP listener
  and draining pending operations.
  """
  @spec stop_nfs_server(nfs_server()) :: :ok | {:error, String.t()}
  def stop_nfs_server(_server), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Reply to an NFS operation.

  Send a reply back to the NFS server for a pending operation.
  The reply format depends on the operation type:
  - `{:ok, data}` — Success with operation-specific data
  - `{:error, errno}` — Error with errno code

  Returns `:ok` if the reply was sent successfully.
  """
  @spec reply_nfs_operation(nfs_server(), request_id(), term()) ::
          :ok | {:error, String.t()}
  def reply_nfs_operation(_server, _request_id, _reply),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Get server statistics.

  Returns `{pending_count, is_shutdown}` for debugging.
  """
  @spec server_stats(nfs_server()) ::
          {:ok, {non_neg_integer(), boolean()}} | {:error, String.t()}
  def server_stats(_server), do: :erlang.nif_error(:nif_not_loaded)
end
