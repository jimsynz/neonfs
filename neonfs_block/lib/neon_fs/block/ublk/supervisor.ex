defmodule NeonFS.Block.Ublk.Supervisor do
  @moduledoc """
  The node's ublk attachments.

  One `NeonFS.Block.Ublk.Target` child per attached export, keyed by export
  name so an attach is idempotent and a detach has something to name. The
  NBD frontend needs no equivalent because its unit of work is a connection
  the client opens; a ublk device is attached by this node on its own
  initiative, so something here has to own it.

  ## `:temporary` children

  A target now survives its helper dying — `USER_RECOVERY` lets it resume the
  quiesced device in place — so a target that *stops* has failed at something
  recovery could not fix: the cluster fenced the attachment, or the helper
  failed so often it exhausted the recovery budget.

  Neither is helped by restarting the target. A fresh target creates a fresh
  device at a fresh `/dev/ublkbN` while the guest holds the old path, and takes
  a new attachment claim to do it — so the restart would publish something
  nothing is using and hide the reason the old one went. Re-attaching is the
  caller's decision, made with that reason in hand.
  """

  use DynamicSupervisor

  alias NeonFS.Block.Ublk.Target

  @registry NeonFS.Block.Ublk.Registry

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Attaches `export` as a ublk device, or returns the target already serving it.
  """
  @spec attach(String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def attach(export, opts \\ []) do
    child = {Target, Keyword.merge(opts, export: export, name: via(export))}

    case DynamicSupervisor.start_child(
           __MODULE__,
           Supervisor.child_spec(child, restart: :temporary)
         ) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Drops the ublk device serving `export`, if this node has one.

  Idempotent: detaching what is not attached is `:ok`, so a caller retrying
  after a target already died of its own accord is not an error.
  """
  @spec detach(String.t()) :: :ok
  def detach(export) do
    case Registry.lookup(@registry, export) do
      [{pid, _value}] -> terminate(pid)
      [] -> :ok
    end
  end

  @doc """
  The exports this node currently serves over ublk.

  Filtered to live targets. A `Registry` entry outlives its process by
  however long the registry takes to handle the `DOWN`, and reporting a
  device whose target has already died would have a caller believe an
  attachment exists that nothing is serving.
  """
  @spec attached() :: [String.t()]
  def attached do
    @registry
    |> Registry.select([{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.filter(fn {_export, pid} -> Process.alive?(pid) end)
    |> Enum.map(fn {export, _pid} -> export end)
  end

  @doc "The registry ublk targets are named in."
  @spec registry() :: atom()
  def registry, do: @registry

  @impl DynamicSupervisor
  def init(_opts), do: DynamicSupervisor.init(strategy: :one_for_one)

  # A target that died on its own accord is already detached, and a caller
  # that asked for that outcome got it.
  defp terminate(pid) do
    case DynamicSupervisor.terminate_child(__MODULE__, pid) do
      {:error, :not_found} -> :ok
      :ok -> :ok
    end
  end

  defp via(export), do: {:via, Registry, {@registry, export}}
end
