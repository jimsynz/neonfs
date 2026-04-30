defmodule NeonFS.Containerd.WriteRegistry do
  @moduledoc """
  Registry mapping in-progress write `ref`s to their `WriteSession`
  GenServer pids. Containerd reuses the same `ref` across reconnects
  to resume a partial write — so the registry has to outlive any
  individual gRPC bidi-stream connection.
  """

  @doc false
  def child_spec(_arg) do
    Registry.child_spec(keys: :unique, name: __MODULE__)
  end

  @doc """
  Look up the GenServer pid for a write ref. Returns `:error` when
  no session is registered.
  """
  @spec lookup(String.t()) :: {:ok, pid()} | :error
  def lookup(ref) do
    case Registry.lookup(__MODULE__, ref) do
      [{pid, _}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc """
  Build the `via` tuple for `name:` — used by `WriteSession.start_link/1`
  so the GenServer registers itself under its `ref`.
  """
  @spec via(String.t()) :: {:via, Registry, {module(), String.t()}}
  def via(ref), do: {:via, Registry, {__MODULE__, ref}}

  @doc """
  List every registered `{ref, pid}` pair. Powers
  `ListStatuses` and the stale-partial sweep.
  """
  @spec list_all() :: [{String.t(), pid()}]
  def list_all do
    Registry.select(__MODULE__, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
  end
end
