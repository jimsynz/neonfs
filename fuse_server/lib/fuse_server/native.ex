defmodule FuseServer.Native do
  @moduledoc """
  Low-level transport bindings for the Linux FUSE kernel ABI.

  This module wraps a raw non-blocking file descriptor (either `/dev/fuse`
  in production or one end of a `pipe(2)` pair in tests) in a Rustler
  resource. Four operations are exposed:

    * `open_dev_fuse/0` — open `/dev/fuse`.
    * `pipe_pair/0` — allocate a pipe pair for testing against hosts
      without FUSE support.
    * `select_read/1` — arm a single read-readiness notification; the
      owning process receives
      `{:select, handle, :undefined, :ready_input}` when the fd is
      readable. Must be re-armed after each notification.
    * `read_frame/1` / `write_frame/2` — non-blocking `read(2)` /
      `write(2)` of a bounded frame (up to 128 KiB — the FUSE kernel
      `max_write`).

  The fd is owned by the resource and closed when the last Erlang
  reference is released. Errors surface as `{:error, atom}` tuples using
  the POSIX errno atoms declared in `t:error/0`.

  This module owns the `enif_select` integration; higher-level FUSE
  protocol handling (opcode dispatch, INIT handshake, backend callbacks)
  lives elsewhere in the future `FuseServer` supervision tree.
  """

  use Rustler,
    otp_app: :fuse_server,
    crate: :fuse_server

  @typedoc """
  Opaque handle wrapping a non-blocking file descriptor. The fd is closed
  when the last Erlang reference is released.
  """
  @type handle :: reference()

  @typedoc """
  Error atoms returned by the transport NIFs. `:eagain` means the fd is
  not currently ready — re-arm via `select_read/1` and wait for the
  `{:select, handle, :undefined, :ready_input}` message.
  """
  @type error ::
          :eagain
          | :eintr
          | :einval
          | :enodev
          | :enoent
          | :enosys
          | :eperm
          | :epipe
          | :select_already_closed
          | :select_failed
          | :select_not_supported

  @doc """
  Open `/dev/fuse` with `O_RDWR | O_NONBLOCK | O_CLOEXEC` and return a
  resource handle owning the fd.
  """
  @spec open_dev_fuse() :: {:ok, handle()} | {:error, error()}
  def open_dev_fuse, do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Allocate a non-blocking `pipe(2)` pair wrapped in the same resource
  type. Primarily a test aid so the select / read / write path can be
  exercised on hosts without FUSE support.

  Returns `{:ok, {read_handle, write_handle}}`.
  """
  @spec pipe_pair() :: {:ok, {handle(), handle()}} | {:error, error()}
  def pipe_pair, do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Arm a single read-readiness notification for `handle`. The calling
  process receives `{:select, handle, :undefined, :ready_input}` when
  the fd becomes readable. The registration is consumed on delivery and
  must be re-armed after each notification.
  """
  @spec select_read(handle()) :: :ok | {:error, error()}
  def select_read(_handle), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Perform a single non-blocking `read(2)` and return the bytes read.
  Returns `{:error, :eagain}` if the fd is not currently readable — in
  that case, re-arm via `select_read/1`.

  The returned binary is bounded at 128 KiB (the FUSE kernel
  `max_write`), which is a protocol-defined ceiling, not a scaling bound
  — so this is not a whole-file-buffering violation.
  """
  @spec read_frame(handle()) :: {:ok, binary()} | {:error, error()}
  def read_frame(_handle), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Perform a single non-blocking `write(2)` of a complete frame. Returns
  `{:error, :eagain}` on a short write.
  """
  @spec write_frame(handle(), binary()) :: :ok | {:error, error()}
  def write_frame(_handle, _frame), do: :erlang.nif_error(:nif_not_loaded)
end
