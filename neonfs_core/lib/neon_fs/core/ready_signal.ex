defmodule NeonFS.Core.ReadySignal do
  @moduledoc """
  Signals that all preceding supervisor children have started successfully.

  This is the **last** child in `NeonFS.Core.Supervisor`. On init it joins
  the `:pg` group `{:node, :ready}` in the `:neonfs_events` scope. Other
  nodes (e.g. the integration test controller) can monitor this group via
  `:pg.monitor/2` for an event-driven readiness check instead of polling.
  """

  use GenServer

  @pg_scope :neonfs_events

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :pg.join(@pg_scope, {:node, :ready}, self())
    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    :pg.leave(@pg_scope, {:node, :ready}, self())
    :ok
  end
end
