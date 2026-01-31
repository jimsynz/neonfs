defmodule NeonFS.FUSE.Native do
  @moduledoc """
  Native Rust NIF bindings for FUSE filesystem operations.

  This module provides low-level bindings to the neonfs_fuse Rust crate,
  which handles FUSE filesystem operations via the fuser library.
  """

  use Rustler,
    otp_app: :neonfs_fuse,
    crate: :neonfs_fuse

  @type fuse_server :: reference()
  @type mount_session :: reference()
  @type request_id :: non_neg_integer()

  @doc """
  Start a FUSE server that communicates with the given callback PID.

  Operations will be sent as messages to the callback process in the format:
  `{:fuse_op, request_id, operation}`

  Returns `{:ok, server}` where server is an opaque reference.
  """
  @spec start_fuse_server(pid()) :: {:ok, fuse_server()} | {:error, String.t()}
  def start_fuse_server(_callback_pid), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Stop a running FUSE server.

  Signals the server to shut down gracefully.
  """
  @spec stop_fuse_server(fuse_server()) :: :ok | {:error, String.t()}
  def stop_fuse_server(_server), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Reply to a FUSE operation.

  Send a reply back to the FUSE thread for a pending operation.
  The reply should be one of:
  - `:ok` - Simple success
  - `{:ok, data}` - Success with data
  - `{:error, errno}` - Error with errno code

  Returns `:ok` if the reply was sent successfully.
  """
  @spec reply_fuse_operation(fuse_server(), request_id(), term()) ::
          :ok | {:error, String.t()}
  def reply_fuse_operation(_server, _request_id, _reply),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Test function to submit a mock operation.

  Used for testing the communication infrastructure.
  """
  @spec test_operation(fuse_server(), String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def test_operation(_server, _operation_type), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Get server statistics.

  Returns `{pending_count, is_shutdown}` for debugging.
  """
  @spec server_stats(fuse_server()) ::
          {:ok, {non_neg_integer(), boolean()}} | {:error, String.t()}
  def server_stats(_server), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Mount a FUSE filesystem at the given mount point.

  This function spawns a FUSE session that handles filesystem operations
  by forwarding them to the callback process. Operations will be sent as
  messages in the format: `{:fuse_op, request_id, operation}`.

  ## Options
  - `"auto_unmount"` - Automatically unmount when the process exits
  - `"allow_other"` - Allow other users to access the filesystem
  - `"allow_root"` - Allow root to access the filesystem
  - `"ro"` - Mount read-only

  Returns `{:ok, session}` where session is an opaque reference to the mount.

  Note: This function requires the "fuse" feature to be enabled at compile time.
  If the feature is not enabled, it will return an error.
  """
  @spec mount(String.t(), pid(), [String.t()]) ::
          {:ok, mount_session()} | {:error, String.t()}
  def mount(_mount_point, _callback_pid, _options),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Unmount a FUSE filesystem.

  Gracefully unmounts the filesystem and cleans up resources.
  This function blocks until the unmount is complete.

  ## Parameters
  - `session` - The mount session returned from `mount/3`
  - `fusermount_cmd` - The fusermount command to use (e.g., "fusermount3")
  """
  @spec unmount(mount_session(), String.t()) :: {:ok, :ok} | {:error, String.t()}
  def unmount(_session, _fusermount_cmd), do: :erlang.nif_error(:nif_not_loaded)
end
