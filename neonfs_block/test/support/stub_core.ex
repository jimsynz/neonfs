defmodule NeonFS.Block.StubCore do
  @moduledoc """
  A `NeonFS.Block.Frontend` implementation that reports rather than stores.

  Every callback tells the test process what it was asked for and answers
  successfully. Shared by both frontends' tests on purpose: the seam's claim
  is that NBD and ublk name no core of their own, and one stub answering both
  is what demonstrates it — two near-identical stubs would let the two
  frontends drift onto different contracts without a test noticing.

  The test process is addressed through application env rather than passed in,
  because the frontends resolve their core through `Frontend.impl/0` and have
  nowhere to thread a pid.
  """

  @behaviour NeonFS.Block.Frontend

  @size 1_048_576
  @block 4096

  @doc "The geometry every stub device reports."
  @spec geometry() :: %{size: pos_integer(), block: pos_integer()}
  def geometry, do: %{size: @size, block: @block}

  @doc "Directs this stub's reports at `pid`."
  @spec report_to(pid()) :: :ok
  def report_to(pid), do: Application.put_env(:neonfs_block, :stub_core_test_pid, pid)

  @impl true
  def open(export) do
    send(test_pid(), {:core, :open, export})

    {:ok,
     %{
       export: export,
       volume: "stub",
       path: "/dev.img",
       id: "stub-id",
       chunk_bytes: @block,
       epoch: 0,
       window: nil,
       size: @size,
       logical_block_size: @block,
       physical_block_size: @block,
       read_only: Application.get_env(:neonfs_block, :stub_core_read_only, false)
     }}
  end

  @impl true
  def export_info(device) do
    %{
      size: device.size,
      logical_block_size: device.logical_block_size,
      physical_block_size: device.physical_block_size,
      read_only: device.read_only
    }
  end

  @impl true
  def read_stream(_device, offset, length) do
    send(test_pid(), {:core, :read_stream, offset, length})
    {:ok, [:binary.copy(<<0xC3>>, length)]}
  end

  @impl true
  def write(_device, offset, data) do
    send(test_pid(), {:core, :write, offset, byte_size(data)})
    :ok
  end

  @impl true
  def flush(_device) do
    send(test_pid(), {:core, :flush})
    :ok
  end

  @impl true
  def write_zeroes(_device, offset, length) do
    send(test_pid(), {:core, :write_zeroes, offset, length})
    :ok
  end

  @impl true
  def measure_read(_device, bytes, _start_time, status) do
    send(test_pid(), {:core, :measure_read, bytes, status})
    :ok
  end

  defp test_pid, do: Application.fetch_env!(:neonfs_block, :stub_core_test_pid)
end
