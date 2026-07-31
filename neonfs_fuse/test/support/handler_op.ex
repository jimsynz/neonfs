defmodule NeonFS.FUSE.TestSupport.HandlerOp do
  @moduledoc """
  Shared deadlines for FUSE handler operations driven from the
  integration suite.

  The suite drives `Handler` by sending `{:fuse_op, tag, {op, params}}`
  and waiting for `{:fuse_op_complete, tag, reply}`. Every file that does
  this needs the same deadline, and each one picking its own is how the
  suite ends up with a file that flakes for a cause its sibling already
  diagnosed and fixed.
  """

  # A handler op that touches core metadata round-trips Ra and the blob
  # store, which takes well over five seconds on a loaded CI runner. The
  # deadline only bounds latency — a wrong reply still fails the pattern
  # match — so a generous value costs nothing except how long a genuinely
  # hung op takes to report.
  @op_timeout 15_000

  @doc """
  Deadline, in milliseconds, for a single handler operation to reply.

  Pass to `assert_receive/2` rather than writing a literal:

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, op_timeout()
  """
  @spec op_timeout() :: pos_integer()
  def op_timeout, do: @op_timeout
end
