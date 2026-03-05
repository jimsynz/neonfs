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
  @spec stop_nfs_server(nfs_server()) :: {:ok, {}} | {:error, String.t()}
  def stop_nfs_server(_server), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Reply to an NFS operation.

  Send a reply back to the NFS server for a pending operation.
  The reply format depends on the operation type:
  - `{:ok, data}` — Success with operation-specific data
  - `{:error, errno}` — Error with errno code

  Returns `{:ok, {}}` if the reply was sent successfully.
  """
  @spec reply_nfs_operation(nfs_server(), request_id(), term()) ::
          {:ok, {}} | {:error, String.t()}
  def reply_nfs_operation(_server, _request_id, _reply),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Get server statistics.

  Returns `{:ok, {pending_count, is_shutdown}}` for debugging.
  """
  @spec server_stats(nfs_server()) ::
          {:ok, {non_neg_integer(), boolean()}} | {:error, String.t()}
  def server_stats(_server), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Get the TCP port the NFS server is listening on.

  Polls until the port is available (up to 5 seconds). Useful for tests
  that start the server on port 0 (OS-assigned).
  """
  @spec get_server_port(nfs_server()) :: {:ok, non_neg_integer()} | {:error, String.t()}
  def get_server_port(_server), do: :erlang.nif_error(:nif_not_loaded)

  # -- Test client NIFs (for protocol tests) --

  @type test_client :: reference()

  @doc false
  @spec test_client_connect(String.t(), non_neg_integer(), String.t()) ::
          {:ok, test_client()} | {:error, String.t()}
  def test_client_connect(_host, _port, _export), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_disconnect(test_client()) :: {:ok, {}} | {:error, String.t()}
  def test_client_disconnect(_client), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_root_handle(test_client()) :: {:ok, binary()} | {:error, String.t()}
  def test_client_root_handle(_client), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_null(test_client()) :: {:ok, {}} | {:error, String.t()}
  def test_client_null(_client), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_getattr(test_client(), binary()) :: {:ok, map()} | {:error, String.t()}
  def test_client_getattr(_client, _fh), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_lookup(test_client(), binary(), String.t()) ::
          {:ok, map()} | {:error, String.t()}
  def test_client_lookup(_client, _dir_fh, _name), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_readdirplus(test_client(), binary()) ::
          {:ok, [map()]} | {:error, String.t()}
  def test_client_readdirplus(_client, _dir_fh), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_create(test_client(), binary(), String.t()) ::
          {:ok, map()} | {:error, String.t()}
  def test_client_create(_client, _dir_fh, _name), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_mkdir(test_client(), binary(), String.t()) ::
          {:ok, map()} | {:error, String.t()}
  def test_client_mkdir(_client, _dir_fh, _name), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_write(test_client(), binary(), non_neg_integer(), binary()) ::
          {:ok, non_neg_integer()} | {:error, String.t()}
  def test_client_write(_client, _fh, _offset, _data), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_read(test_client(), binary(), non_neg_integer(), non_neg_integer()) ::
          {:ok, map()} | {:error, String.t()}
  def test_client_read(_client, _fh, _offset, _count), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_remove(test_client(), binary(), String.t()) ::
          {:ok, {}} | {:error, String.t()}
  def test_client_remove(_client, _dir_fh, _name), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_setattr(
          test_client(),
          binary(),
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil
        ) :: {:ok, map()} | {:error, String.t()}
  def test_client_setattr(_client, _fh, _mode, _uid, _gid, _size),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_rename(test_client(), binary(), String.t(), binary(), String.t()) ::
          {:ok, {}} | {:error, String.t()}
  def test_client_rename(_client, _from_dir_fh, _from_name, _to_dir_fh, _to_name),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_symlink(test_client(), binary(), String.t(), String.t()) ::
          {:ok, map()} | {:error, String.t()}
  def test_client_symlink(_client, _dir_fh, _name, _target),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_readlink(test_client(), binary()) :: {:ok, String.t()} | {:error, String.t()}
  def test_client_readlink(_client, _fh), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  @spec test_client_create_exclusive(test_client(), binary(), String.t()) ::
          {:ok, map()} | {:error, String.t()}
  def test_client_create_exclusive(_client, _dir_fh, _name),
    do: :erlang.nif_error(:nif_not_loaded)
end
