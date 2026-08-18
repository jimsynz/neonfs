defmodule NeonFS.Block.Frontend do
  @moduledoc """
  What a block frontend needs of the IO core.

  A frontend is whatever speaks a block protocol to a client — NBD today,
  `ublk` next. It owns framing, its own request lifecycle, and nothing else:
  every operation a client can ask for is one of the callbacks here, answered
  by the IO core against a cluster.

  ## Why the seam is here

  Before this, `NeonFS.Block.ConnectionHandler` named `NeonFS.Block.Device`
  functions directly, so NBD's transmission loop and the IO core were one
  layer. A second frontend would have had to either call `Device` itself —
  duplicating the parts that are not protocol at all, such as retrying a
  contended span — or refactor NBD while introducing itself, which makes a
  regression in either look like the other.

  ## What belongs on which side

  The dividing question is whether a client could observe it.

    * **Frontend**: framing, error *codes* (NBD's `EAGAIN` has no counterpart
      in `ublk`), request identifiers, how a reply is written to a socket.
    * **IO core**: what a read or write means, how contention is handled, and
      the telemetry that describes cost rather than protocol.

  `retrying_stale/3` is the load-bearing example. A write that exhausted core's
  retry budget against a contended span has lost nothing, and every frontend
  wants it retried rather than failed — NBD because its error set has no
  "retry" status, `ublk` because `-EAGAIN` there means something else again.
  Retrying is therefore the core's, and only the *reply* is the frontend's.

  ## Implementations

    * `NeonFS.Block.Device` — the cluster-backed core, proven by the rig's
      block steps.
  """

  alias NeonFS.Block.Device

  @typedoc "A resolved device handle, opaque to a frontend."
  @type device :: Device.t()

  @typedoc """
  Geometry a frontend advertises to its client: size in bytes and the two
  block sizes, which every block protocol needs and each spells differently.
  """
  @type export_info :: %{
          size: non_neg_integer(),
          logical_block_size: pos_integer(),
          physical_block_size: pos_integer(),
          read_only: boolean()
        }

  @doc "Resolves an export name (`<volume>` or `<volume>:<path>`) into a handle."
  @callback open(export :: String.t()) :: {:ok, device()} | {:error, term()}

  @doc "Geometry to advertise for an open device."
  @callback export_info(device()) :: export_info()

  @doc """
  A lazy stream of the range's bytes, one element per chunk.

  Streaming rather than a binary is the contract: a frontend writes each
  element as it arrives, so the largest request an export advertises costs one
  chunk of memory rather than the range.
  """
  @callback read_stream(device(), offset :: non_neg_integer(), length :: pos_integer()) ::
              {:ok, Enumerable.t()} | {:error, term()}

  @doc "Writes `data` at `offset`, retrying a contended span before answering."
  @callback write(device(), offset :: non_neg_integer(), data :: binary()) ::
              :ok | {:error, term()}

  @doc """
  Returns only once everything acknowledged is durable.

  Acknowledging earlier would tell a guest filesystem its journal is safe when
  it is not, which is the one thing a block device must never do.
  """
  @callback flush(device()) :: :ok | {:error, term()}

  @doc "Zero-fills a range — TRIM and WRITE ZEROES both land here."
  @callback write_zeroes(device(), offset :: non_neg_integer(), length :: pos_integer()) ::
              :ok | {:error, term()}

  @doc """
  Emits a read's telemetry once its stream has been drained.

  A read's byte count is only known to whoever consumed the stream, which is
  the frontend — so unlike every other command, the core cannot measure it
  alone.
  """
  @callback measure_read(
              device(),
              bytes :: non_neg_integer(),
              start_time :: integer(),
              status :: :ok | :error
            ) :: :ok

  @doc """
  The IO core in use.

  Indirected through application env so a test can drive a frontend against a
  stub core without a cluster behind it — the same seam
  `:coordinator_call_fn` uses for the claim layer.
  """
  @spec impl() :: module()
  def impl, do: Application.get_env(:neonfs_block, :io_core, Device)
end
