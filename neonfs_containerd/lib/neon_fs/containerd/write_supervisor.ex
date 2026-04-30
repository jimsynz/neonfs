defmodule NeonFS.Containerd.WriteSupervisor do
  @moduledoc """
  Dynamic supervisor for `WriteSession` GenServers — one child per
  in-progress write `ref`. Sessions live until they're explicitly
  committed, aborted, or time-out from inactivity.
  """

  use DynamicSupervisor

  alias NeonFS.Containerd.WriteSession

  @doc false
  def start_link(_init_arg) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Start (or look up) a `WriteSession` for `ref`. Idempotent — if the
  session already exists for this ref, returns its pid rather than
  failing with `:already_started`. That's the resume case (containerd
  reconnecting after a transient disconnect).
  """
  @spec start_session(String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_session(ref, opts \\ []) do
    spec = {WriteSession, [ref: ref] ++ opts}

    case DynamicSupervisor.start_child(__MODULE__, spec) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, _} = err ->
        err
    end
  end
end
