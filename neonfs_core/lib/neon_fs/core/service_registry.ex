defmodule NeonFS.Core.ServiceRegistry do
  @moduledoc """
  Registry for cluster service instances.

  Tracks which nodes are running which services (core, fuse, nfs, s3,
  webdav, docker, iam).
  Backed entirely by Ra: reads go through `RaSupervisor.local_query/2`
  against the `MetadataStateMachine`; writes go through Raft consensus
  via `RaSupervisor.command/2`. There is no ETS cache — every query
  hits the locally-committed state, so registrations replicated by the
  leader are immediately visible on every follower.

  A single BEAM node may host multiple NeonFS services, so registry
  entries are keyed by `{node, type}` rather than node alone.

  The GenServer remains in the supervision tree because it still owns:
  self-registration on startup (`handle_continue(:register_self, …)`),
  re-registration on data-plane endpoint changes (`refresh_self/0`),
  node-down monitoring via `:net_kernel.monitor_nodes/2`, a periodic
  check that this node's own `:core` entry is still present, and
  best-effort deregistration of the local core service on shutdown.

  That periodic check is what gives a core node the resilience an
  interface node gets from `NeonFS.Client.Registrar`'s unconditional
  re-registration: this node is not the only writer of its own entry, so
  retrying its own failed writes is not enough. It polls a local Ra query
  every `:service_registry_self_heal_interval` milliseconds (default
  5,000) and writes only when the entry is missing.
  """

  use GenServer
  require Logger

  alias NeonFS.Client.{ServiceInfo, ServiceType}
  alias NeonFS.Cluster.State
  alias NeonFS.Core.{MetadataStateMachine, NodeRegistry, RaServer, RaSupervisor}
  alias NeonFS.Transport.{Listener, PoolManager}

  @core_probe_timeout_ms 1_000

  # Deliberately an order of magnitude tighter than `maybe_ra_command/2`'s
  # 5s default, because the budget that constrains a registration is not
  # `register/1`'s own 10s `GenServer.call` — it is the enclosing operation's.
  # `Cluster.Join.join_cluster_rpc/3` registers the joining service as one step
  # among many inside a single RPC whose callers bound the whole join (the test
  # harness at 30s), and node boot self-registers on the way up. Spending 5s
  # here on a write that is going to fail anyway pushes those over their
  # budgets; raising it to 5s reproducibly timed out the join RPC in CI, and
  # the caller then retried a join that had already landed.
  #
  # The cost of the short deadline is a write reported as failed that Ra later
  # commits. That is acceptable because every caller treats a registration
  # failure as retryable, not fatal: `NeonFS.Client.Registrar` re-issues every
  # 5s, and the self-registration paths log and are re-driven.
  @ra_write_timeout_ms 500

  # On boot the data-plane `Listener` may not have bound yet when we first
  # self-register, so `build_self_metadata/0` yields no `:data_endpoint`
  # and peers can't open a data-plane pool to us. The init/join
  # flows call `refresh_self/0` once the Listener is up, but a plain
  # auto-restart from persisted state runs neither — so we self-heal by
  # re-registering until the endpoint is present.
  # Unbounded, because "the Listener has not bound" has no attempt count at
  # which giving up is the better answer: the node stays registered, so no
  # client reports it unreachable, but no peer can open a data-plane pool to
  # it — silent and permanent, which is the worst pair. It used to stop after
  # 60 tries.
  #
  # Retrying forever is affordable because a tick that finds no endpoint does
  # not write. `build_self_metadata/0` is a local check; only the tick that
  # finds an endpoint issues a Ra command. The old chain re-registered on
  # every tick, so a slow Listener cost 60 Ra writes per boot.
  @endpoint_retry_initial_ms 1_000
  @endpoint_retry_max_ms 30_000
  @endpoint_retry_max_doublings 5

  # Ticks before saying so out loud. At the backoff above this is roughly a
  # minute in — the point at which the old code silently gave up.
  @endpoint_missing_warn_after 6

  # A `:register_service` command that fails is a separate problem from a
  # missing endpoint, and needs its own retry: until it commits, this node is
  # absent from every other node's registry and clients report "all core nodes
  # unreachable". Nothing else re-drives it — an interface node re-registers
  # through `NeonFS.Client.Registrar` every 5s, but a core node has no
  # heartbeat, and `refresh_self/0` runs only from `cluster init` and the join
  # flow, neither of which happens on a plain restart.
  #
  # So the attempt count is unbounded; there is no number at which an
  # undiscoverable core node is the better outcome. What's bounded is the
  # delay, which backs off to a ceiling so a node that has been failing for
  # minutes stops hammering Ra at boot cadence.
  @write_retry_initial_ms 500
  @write_retry_max_ms 30_000
  @write_retry_max_doublings 8

  # Retrying our own failed writes is not enough, because we are not the only
  # writer of our registration: a peer that sees us go down deregisters every
  # service on this node, and nothing puts it back. An interface node survives
  # that because `NeonFS.Client.Registrar` re-registers unconditionally every
  # 5s; a core node had no equivalent, so a transient split left it absent from
  # every registry — including its own — for the rest of its uptime.
  #
  # This is the missing heartbeat, but conditional rather than unconditional:
  # the check is a local Ra query, and a write only goes out when this node is
  # actually missing. In the steady state that costs no Raft log entries at
  # all, which an unconditional re-register at `Registrar`'s cadence would not.
  @self_heal_interval_ms 5_000

  ## Client API

  @doc """
  Starts the service registry.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Deregisters all services for a node from the service registry.
  """
  @spec deregister(node()) :: :ok | {:error, term()}
  def deregister(node) do
    GenServer.call(__MODULE__, {:deregister, node, nil}, 10_000)
  end

  @doc """
  Deregisters a specific service for a node from the service registry.
  """
  @spec deregister(node(), ServiceType.t()) :: :ok | {:error, term()}
  def deregister(node, type) do
    GenServer.call(__MODULE__, {:deregister, node, type}, 10_000)
  end

  @doc """
  Re-registers this node's service info with updated metadata.

  Call this after TLS certificates are written and `Listener.rebind/0`
  succeeds, so the new data transfer endpoint is advertised.
  """
  @spec refresh_self() :: :ok
  def refresh_self do
    GenServer.cast(__MODULE__, :refresh_self)
  end

  @doc """
  Gets service info for a specific node. Returns the `:core` service
  if one is registered, otherwise the first (alphabetical by type) of
  whatever is registered on that node.
  """
  @spec get(node()) :: {:ok, ServiceInfo.t()} | {:error, :not_found}
  def get(node) do
    case list_by_node(node) do
      [] -> {:error, :not_found}
      [info] -> {:ok, info}
      services -> {:ok, Enum.find(services, &(&1.type == :core)) || hd(services)}
    end
  end

  @doc """
  Gets service info for a specific node and type.
  """
  @spec get(node(), ServiceType.t()) :: {:ok, ServiceInfo.t()} | {:error, :not_found}
  def get(node, type) do
    case read_service(node, type) do
      {:ok, service_map} when is_map(service_map) -> {:ok, ServiceInfo.from_map(service_map)}
      {:ok, nil} -> {:error, :not_found}
      {:error, _} -> {:error, :not_found}
    end
  end

  @doc """
  Lists all registered services.
  """
  @spec list() :: [ServiceInfo.t()]
  def list do
    case read_services() do
      {:ok, services_map} ->
        draining = NodeRegistry.draining_nodes()
        maintenance = NodeRegistry.maintenance_nodes()

        services_map
        |> Map.values()
        |> Enum.map(&ServiceInfo.from_map/1)
        |> Enum.map(&mark_lifecycle(&1, draining, maintenance))
        |> Enum.sort_by(&{&1.node, &1.type})

      {:error, _} ->
        []
    end
  end

  # Stamp the node's lifecycle status on its services, so
  # the existing discovery RPC carries node lifecycle to clients without
  # a new protocol. The client `CostFunction` deprioritises both
  # `:draining` and `:maintenance` nodes when routing.
  defp mark_lifecycle(%ServiceInfo{node: node} = info, draining, maintenance) do
    cond do
      MapSet.member?(draining, node) -> %{info | status: :draining}
      MapSet.member?(maintenance, node) -> %{info | status: :maintenance}
      true -> info
    end
  end

  @doc """
  Lists services running on a given node.
  """
  @spec list_by_node(node()) :: [ServiceInfo.t()]
  def list_by_node(node) do
    list()
    |> Enum.filter(&(&1.node == node))
    |> Enum.sort_by(& &1.type)
  end

  @doc """
  Lists services of a given type.
  """
  @spec list_by_type(ServiceType.t()) :: [ServiceInfo.t()]
  def list_by_type(type) do
    Enum.filter(list(), &(&1.type == type))
  end

  @doc """
  Lists currently connected remote nodes for a given service type.
  """
  @spec connected_nodes_by_type(ServiceType.t()) :: [node()]
  def connected_nodes_by_type(:core) do
    connected_nodes = Node.list()

    list_by_type(:core)
    |> Enum.map(& &1.node)
    |> Kernel.++(connected_core_fallback_nodes(connected_nodes))
    |> Enum.uniq()
    |> Enum.filter(&(&1 in connected_nodes))
    |> Enum.sort()
  end

  def connected_nodes_by_type(type) do
    connected_nodes = Node.list()

    list_by_type(type)
    |> Enum.map(& &1.node)
    |> Kernel.++(connected_app_nodes(type, connected_nodes))
    |> Enum.uniq()
    |> Enum.filter(&(&1 in connected_nodes))
    |> Enum.sort()
  end

  @doc """
  Registers a service in the cluster.

  Returns `{:error, reason}` when the registration could not be replicated,
  so a caller is never told a service is visible cluster-wide when it is not.
  A cluster whose Ra server has not been initialised yet is not a failure —
  there is nowhere to write and nothing to be visible to.
  """
  @spec register(ServiceInfo.t()) :: :ok | {:error, term()}
  def register(%ServiceInfo{} = info) do
    GenServer.call(__MODULE__, {:register, info}, 10_000)
  end

  @doc """
  Selects a core node from registered services.
  """
  @spec select_core_node() :: {:ok, node()} | {:error, :no_core_nodes}
  def select_core_node do
    case list_by_type(:core) do
      [first | _] -> {:ok, first.node}
      [] -> {:error, :no_core_nodes}
    end
  end

  @doc """
  Updates metrics for a node.
  """
  @spec update_metrics(node(), map()) :: :ok | {:error, term()}
  def update_metrics(node, metrics) do
    GenServer.call(__MODULE__, {:update_metrics, node, metrics}, 10_000)
  end

  ## Server callbacks

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)
    :net_kernel.monitor_nodes(true, node_type: :visible)
    {:ok, %{monitors: %{}, retry_pending?: false}, {:continue, :register_self}}
  end

  @impl true
  def handle_continue(:register_self, state) do
    schedule_self_heal()
    {:noreply, self_register(state, 0)}
  end

  @impl true
  def terminate(_reason, _state) do
    Logger.info("ServiceRegistry shutting down, deregistering core service")

    command = {:deregister_service, Node.self(), :core}

    case write_result(maybe_ra_command(command, @ra_write_timeout_ms)) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.debug("Failed to deregister core service", reason: inspect(reason))
    end

    :ok
  end

  @impl true
  def handle_cast(:refresh_self, state) do
    {:noreply, self_register(state, 0)}
  end

  @impl true
  def handle_call({:register, info}, _from, state) do
    {result, new_state} = do_register(info, state)
    {:reply, result, new_state}
  end

  @impl true
  def handle_call({:deregister, node, type}, _from, state) do
    {result, new_state} = do_deregister(node, type, state)
    {:reply, result, new_state}
  end

  @impl true
  def handle_call({:update_metrics, node, metrics}, _from, state) do
    reply = do_update_metrics(node, metrics)
    {:reply, reply, state}
  end

  # A nodedown for the local node is spurious — the registry cannot be
  # processing this message if its own node is genuinely down. It fires
  # transiently around the distribution restart on `cluster init`, and
  # deregistering self would strand a single-node cluster with no
  # registered core, so every interface reports "all core nodes
  # unreachable". Ignore it; the local core stays registered.
  @impl true
  def handle_info({:nodedown, node, _info}, state) when node == node() do
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) when node == node() do
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node, _info}, state) do
    Logger.debug("Service node down, deregistering", node: node)
    {:noreply, deregister_and_log(node, nil, state)}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    Logger.debug("Service node down, deregistering", node: node)
    {:noreply, deregister_and_log(node, nil, state)}
  end

  # Nothing to do here. It's tempting to re-register on nodeup, since a peer's
  # nodedown is what deregistered us — but that only covers the heal, and it
  # races the very deregistration it is meant to undo (the peer's command can
  # commit after the link is back). `:self_heal` covers every cause, including
  # that ordering, so this stays empty.
  @impl true
  def handle_info({:nodeup, _node, _info}, state) do
    {:noreply, state}
  end

  def handle_info(:self_heal, state) do
    schedule_self_heal()
    {:noreply, heal_self_registration(state)}
  end

  def handle_info({:retry_register_self, :endpoint, attempt}, state) do
    {:noreply, poll_for_endpoint(state, attempt)}
  end

  def handle_info({:retry_register_self, _cause, attempt}, state) do
    {:noreply, self_register(state, attempt)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Private helpers

  defp schedule_self_heal do
    interval =
      Application.get_env(
        :neonfs_core,
        :service_registry_self_heal_interval,
        @self_heal_interval_ms
      )

    Process.send_after(self(), :self_heal, interval)
    :ok
  end

  # A self-registration already in flight re-writes the record on its own
  # schedule, so healing on top of it would start a second retry chain — and a
  # third five seconds later, since the node stays absent while they run.
  defp heal_self_registration(%{retry_pending?: true} = state), do: state

  # An uninitialised Ra answers a local query from an empty state machine, so
  # "absent" reads the same as "there is no cluster yet". Writes are no-ops in
  # that state, so healing would do nothing but log once every interval.
  #
  # Only the *absence* of the registration is worth acting on. Metadata drift
  # is not: `refresh_self/0` owns that, and re-writing on every difference
  # would fight `update_metrics/2`, which writes fields this process does not
  # know about.
  defp heal_self_registration(state) do
    with true <- RaServer.initialized?(),
         {:ok, nil} <- read_service(Node.self(), :core) do
      reassert_self_registration(state)
    else
      _ -> state
    end
  end

  defp reassert_self_registration(state) do
    Logger.warning("Core service missing from the registry, re-registering")

    :telemetry.execute(
      [:neonfs, :service_registry, :self_registration_healed],
      %{},
      %{node: Node.self()}
    )

    self_register(state, 0)
  end

  # The one path that registers this node's own `:core` service. No caller is
  # waiting on the result, so a failure is only reportable — which is why it
  # has to schedule its own next attempt rather than assume something else
  # will.
  defp self_register(state, attempt) do
    metadata = build_self_metadata()
    info = ServiceInfo.new(Node.self(), :core, metadata: metadata)
    {result, new_state} = do_register(info, state)

    report_self_registration(result, metadata, attempt)

    cause = next_retry_cause(metadata, result, attempt)
    schedule_self_register_retry(cause, attempt)

    %{new_state | retry_pending?: cause != :none}
  end

  # An endpoint tick is a *poll*, not a re-registration. The registration
  # already landed — it just advertised no `:data_endpoint` because the
  # `Listener` had not bound yet — so there is nothing new to say until one
  # exists. Writing anyway is what made the old bounded chain expensive
  # enough to need a bound.
  #
  # Once the endpoint appears, hand off to `self_register/2`, which writes it
  # and ends the chain.
  defp poll_for_endpoint(state, attempt) do
    metadata = build_self_metadata()

    if Map.has_key?(metadata, :data_endpoint) do
      self_register(state, attempt)
    else
      warn_endpoint_missing(attempt)
      schedule_self_register_retry(:endpoint, attempt)
      %{state | retry_pending?: true}
    end
  end

  # The old chain went quiet here. Say it instead — a node nobody can open a
  # data-plane pool to looks healthy from every other angle, so this line is
  # the only signal an operator gets.
  defp warn_endpoint_missing(attempt) when attempt == @endpoint_missing_warn_after do
    Logger.warning(
      "Data-plane listener still has not bound; peers cannot open a pool to this node",
      attempt: attempt
    )

    :telemetry.execute(
      [:neonfs, :service_registry, :self_endpoint_missing],
      %{attempt: attempt},
      %{node: Node.self()}
    )
  end

  defp warn_endpoint_missing(_attempt), do: :ok

  # The two causes are distinct and are checked in this order.
  #
  # `:write` — the command did not commit. Unbounded, with backoff: until it
  # lands this node is absent from every registry and clients report "all core
  # nodes unreachable". Every tick reissues the write.
  #
  # `:endpoint` — the registration committed but advertises no data-plane
  # endpoint, because the `Listener` had not bound yet. Also unbounded, but
  # every tick is a *local* poll and only the one that finds an endpoint
  # writes.
  #
  # A failed write is checked first because an endpoint tick no longer
  # reissues it. Ordering these the other way round would leave a node whose
  # write failed *and* whose Listener is slow polling forever without ever
  # retrying the registration — unregistered, and quietly so.
  defp next_retry_cause(metadata, result, attempt)

  defp next_retry_cause(_metadata, {:error, _reason}, _attempt), do: :write

  defp next_retry_cause(metadata, :ok, _attempt)
       when not is_map_key(metadata, :data_endpoint),
       do: :endpoint

  defp next_retry_cause(_metadata, :ok, _attempt), do: :none

  defp schedule_self_register_retry(:none, _attempt), do: :ok

  defp schedule_self_register_retry(cause, attempt) do
    delay = retry_delay(cause, attempt)

    :telemetry.execute(
      [:neonfs, :service_registry, :self_register_retry_scheduled],
      %{attempt: attempt, delay_ms: delay},
      %{node: Node.self(), cause: cause}
    )

    Process.send_after(self(), {:retry_register_self, cause, attempt + 1}, delay)

    :ok
  end

  defp retry_delay(:endpoint, attempt) do
    min(
      @endpoint_retry_initial_ms * 2 ** min(attempt, @endpoint_retry_max_doublings),
      @endpoint_retry_max_ms
    )
  end

  defp retry_delay(:write, attempt) do
    min(
      @write_retry_initial_ms * 2 ** min(attempt, @write_retry_max_doublings),
      @write_retry_max_ms
    )
  end

  defp report_self_registration({:error, reason}, _metadata, attempt) do
    Logger.warning("Core self-registration failed, will retry",
      reason: inspect(reason),
      attempt: attempt
    )

    :telemetry.execute(
      [:neonfs, :service_registry, :self_registration_failed],
      %{attempt: attempt},
      %{node: Node.self(), reason: reason}
    )
  end

  defp report_self_registration(:ok, metadata, attempt) do
    :telemetry.execute(
      [:neonfs, :service_registry, :self_registered],
      %{attempt: attempt},
      %{node: Node.self(), data_endpoint: Map.get(metadata, :data_endpoint)}
    )
  end

  defp build_self_metadata do
    case Process.whereis(Listener) do
      nil ->
        %{}

      _pid ->
        port = Listener.get_port()

        if port > 0 do
          endpoint = PoolManager.advertise_endpoint(port)
          %{data_endpoint: endpoint}
        else
          %{}
        end
    end
  rescue
    _ -> %{}
  end

  defp connected_app_nodes(type, connected_nodes) do
    Enum.filter(connected_nodes, &service_app_started?(&1, type))
    |> Enum.sort()
  end

  defp connected_core_fallback_nodes(connected_nodes) do
    Enum.filter(connected_nodes, &core_node?/1)
  end

  defp core_node?(node) do
    service_app_started?(node, :core) and cluster_member?(node)
  end

  # Membership rather than the presence of a `cluster.json`: a host provisioned
  # for distribution only holds one of those without being a member, and
  # counting it as a core node would route metadata at something that serves
  # none.
  defp cluster_member?(node) do
    :erpc.call(node, State, :member?, [], @core_probe_timeout_ms)
  catch
    _, _ -> false
  end

  defp service_app_started?(node, type) do
    app = service_application(type)

    node
    |> :erpc.call(Application, :started_applications, [], @core_probe_timeout_ms)
    |> Enum.any?(fn {started_app, _desc, _vsn} -> started_app == app end)
  catch
    _, _ -> false
  end

  defp service_application(type), do: String.to_atom("neonfs_#{type}")

  defp do_register(info, state) do
    info_map = ServiceInfo.to_map(info)

    result =
      {:register_service, info_map}
      |> maybe_ra_command(@ra_write_timeout_ms)
      |> write_result()

    {result, maybe_monitor_node(info.node, state)}
  end

  defp write_result({:ok, :ok}), do: :ok

  # Ra not being initialised is not a write failure: before `cluster init`
  # there is no cluster to be visible to and nowhere to write, which is how
  # every read in this module treats it too.
  defp write_result({:error, :ra_not_available}), do: :ok
  defp write_result({:error, reason}), do: {:error, reason}

  # The state machine only ever replies `:ok` to these commands. Answering
  # rather than raising keeps a future reply shape from taking down a
  # GenServer the whole cluster's discovery depends on.
  defp write_result({:ok, unexpected}), do: {:error, {:unexpected_reply, unexpected}}

  defp maybe_monitor_node(node, state) do
    # Only set up a node monitor if the node is currently connected.
    # `:erlang.monitor_node/2` on an unreachable node fires `:nodedown`
    # immediately, which would synchronously deregister a service we
    # just registered — the entry would disappear from Ra between the
    # caller's `register/1` returning and the next read.
    cond do
      node == Node.self() ->
        state

      Map.has_key?(state.monitors, node) ->
        state

      node not in Node.list() ->
        state

      true ->
        ref = Node.monitor(node, true)
        put_in(state.monitors[node], ref)
    end
  end

  defp do_deregister(node, type, state) do
    command = if type, do: {:deregister_service, node, type}, else: {:deregister_service, node}

    result =
      command
      |> maybe_ra_command(@ra_write_timeout_ms)
      |> write_result()

    # Membership-change observers (e.g. `ReplicaRepairScheduler` from
    # attach to this telemetry event to react to nodes leaving
    # the cluster without taking a hard dependency on
    # `ServiceRegistry`.
    :telemetry.execute(
      [:neonfs, :service_registry, :service_deregistered],
      %{},
      %{node: node, type: type}
    )

    # Only drop the node-monitor when no other services remain registered
    # for this node — a fuse-only node stays monitored for future nodedowns.
    keep_monitor? = list_by_node(node) != []

    state =
      case {keep_monitor?, Map.pop(state.monitors, node)} do
        {true, {_ref, _new_monitors}} ->
          state

        {_, {nil, _}} ->
          state

        {false, {ref, new_monitors}} when is_reference(ref) ->
          Process.demonitor(ref)
          %{state | monitors: new_monitors}

        {false, {_, new_monitors}} ->
          %{state | monitors: new_monitors}
      end

    {result, state}
  end

  # The nodedown-driven deregistration has no caller to answer, and the entry
  # it is cleaning up belongs to a node that has already gone — a failure here
  # leaves a stale registration that the departed node's own re-registration
  # (or the next nodedown) resolves.
  defp deregister_and_log(node, type, state) do
    {result, new_state} = do_deregister(node, type, state)

    case result do
      :ok -> :ok
      {:error, reason} -> Logger.warning("Ra deregister_service failed", reason: inspect(reason))
    end

    new_state
  end

  defp do_update_metrics(node, metrics) do
    {:update_service_metrics, node, metrics}
    |> maybe_ra_command(@ra_write_timeout_ms)
    |> write_result()
  end

  defp read_service(node, type) do
    RaSupervisor.local_query(&MetadataStateMachine.get_service(&1, node, type))
  catch
    :exit, _ -> {:error, :ra_not_available}
  end

  defp read_services do
    RaSupervisor.local_query(&MetadataStateMachine.get_services/1)
  catch
    :exit, _ -> {:error, :ra_not_available}
  end

  defp maybe_ra_command(cmd, timeout) do
    if RaServer.initialized?() do
      maybe_ra_command_impl(cmd, timeout)
    else
      {:error, :ra_not_available}
    end
  end

  defp maybe_ra_command_impl(cmd, timeout) do
    case RaSupervisor.command(cmd, timeout) do
      # `:ra.process_command/3` answers `{:ok, Reply, Leader}` once the command
      # commits, whatever `Reply` is — so a state-machine rejection arrives
      # under an outer `:ok`. This clause must precede the generic one or the
      # rejection reads as a success.
      {:ok, {:error, reason}, _leader} ->
        {:error, reason}

      {:ok, result, _leader} ->
        {:ok, result}

      {:error, :noproc} ->
        if RaServer.initialized?() do
          {:error, :ra_unavailable}
        else
          {:error, :ra_not_available}
        end

      {:error, reason} ->
        {:error, reason}

      {:timeout, _node} ->
        {:error, :timeout}
    end
  catch
    :exit, {:noproc, _} ->
      if RaServer.initialized?() do
        {:error, :ra_unavailable}
      else
        {:error, :ra_not_available}
      end

    kind, reason ->
      Logger.debug("Ra command error", kind: kind, reason: reason)

      if RaServer.initialized?() do
        {:error, {:ra_error, {kind, reason}}}
      else
        {:error, :ra_not_available}
      end
  end
end
