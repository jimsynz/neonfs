defmodule NeonFS.CSI.AttachHolder do
  @moduledoc """
  Stable holder pid for `NeonFS.Core.NamespaceCoordinator` claims that
  represent this node's block attachments.

  The coordinator monitors the holder pid a claim was taken with and
  releases every claim that holder owns when the monitor fires. That is the
  whole mechanism behind "a dead node releases its attachment": the claim's
  lifetime is a process's lifetime, and the process has to live on the node
  the attachment belongs to.

  It cannot be the process that takes the claim. A controller reaches the
  coordinator through `NeonFS.Client.Router.call/4`, which runs on a
  short-lived process on the core node — its `self()` dies the moment the
  call returns, taking the claim with it and releasing an attachment that
  very much still exists.

  So this GenServer exists purely to be a pid: it owns no state, runs on
  every node-mode CSI plugin, and is shared by every attachment on that
  node. `NeonFS.WebDAV.LockStore.NamespaceHolder` is the same idea for
  WebDAV's namespace claims.
  """

  use GenServer

  @doc """
  Starts the holder. The default registered name is the module, so a
  lookup from another node needs only the node name.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  The holder pid on `node`, or an error naming the node that has none.

  A node that is connected but has no holder is a node whose CSI plugin is
  not running in node mode — claiming against it would monitor nothing.
  """
  @spec pid_on(node()) :: {:ok, pid()} | {:error, term()}
  def pid_on(node) when is_atom(node) do
    case :erpc.call(node, Process, :whereis, [__MODULE__], 5_000) do
      pid when is_pid(pid) -> {:ok, pid}
      nil -> {:error, {:no_attach_holder, node}}
    end
  catch
    :error, reason -> {:error, {:attach_holder_unreachable, node, reason}}
    :exit, reason -> {:error, {:attach_holder_unreachable, node, reason}}
  end

  @impl true
  def init(_opts), do: {:ok, %{}}
end
