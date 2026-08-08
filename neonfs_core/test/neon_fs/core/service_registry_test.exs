defmodule NeonFS.Core.ServiceRegistryTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase
  use Mimic

  require Logger

  alias NeonFS.Client.ServiceInfo
  alias NeonFS.Core.{NodeRegistry, RaServer, RaSupervisor, ServiceRegistry}
  alias NeonFS.Transport.{Listener, PoolManager}

  @moduletag :tmp_dir

  # Kept in step with `NeonFS.TestSupport.PeerCluster`, which shares this
  # file's retry classifier. The budget is about how loaded the host is, not
  # how much work a boot does: this module runs under `mix test --partitions 2`
  # on a runner shared with an integration shard, which is the sustained load
  # the widening was written for. The cap only ever lands on the failure path.
  @peer_boot_attempts 8
  @peer_boot_backoff_ms 250
  @peer_boot_max_backoff_ms 5_000

  # How long the self-registration chain may stay silent before it counts as
  # stuck rather than slow. Added to whatever delay the chain last announced,
  # never used as a budget for the whole chain.
  @registration_quiet_ms 5_000

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()

    start_service_registry()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  test "connected_nodes_by_type/1 returns only connected nodes of the requested type" do
    core_port = allocate_port()
    nfs_port = allocate_port()

    peer_ports_env =
      "neonfs_core_registry_peer@localhost:#{core_port},neonfs_nfs_registry_peer@localhost:#{nfs_port}"

    System.put_env("NEONFS_PEER_PORTS", peer_ports_env)

    {core_peer, core_node} =
      start_test_peer(:neonfs_core_registry_peer, core_port, peer_ports_env)

    {nfs_peer, nfs_node} = start_test_peer(:neonfs_nfs_registry_peer, nfs_port, peer_ports_env)

    on_exit(fn ->
      System.delete_env("NEONFS_PEER_PORTS")
      safe_stop_peer(core_peer)
      safe_stop_peer(nfs_peer)
    end)

    Node.connect(core_node)
    Node.connect(nfs_node)

    register_service!(ServiceInfo.new(core_node, :core))
    register_service!(ServiceInfo.new(nfs_node, :nfs))
    register_service!(ServiceInfo.new(:neonfs_core_disconnected@localhost, :core))

    assert ServiceRegistry.connected_nodes_by_type(:core) == [core_node]
    assert ServiceRegistry.connected_nodes_by_type(:nfs) == [nfs_node]
  end

  test "stores multiple services for the same node independently" do
    shared_node = :shared_services@localhost

    register_service!(ServiceInfo.new(shared_node, :core))
    register_service!(ServiceInfo.new(shared_node, :nfs))

    assert {:ok, core_service} = ServiceRegistry.get(shared_node, :core)
    assert {:ok, nfs_service} = ServiceRegistry.get(shared_node, :nfs)
    assert core_service.type == :core
    assert nfs_service.type == :nfs

    assert Enum.map(ServiceRegistry.list_by_node(shared_node), & &1.type) == [:core, :nfs]

    assert :ok = ServiceRegistry.deregister(shared_node, :nfs)
    assert {:ok, _core_service} = ServiceRegistry.get(shared_node, :core)
    assert {:error, :not_found} = ServiceRegistry.get(shared_node, :nfs)
  end

  test "ignores a nodedown for the local node and keeps its services registered" do
    register_service!(ServiceInfo.new(Node.self(), :core))
    assert {:ok, _} = ServiceRegistry.get(Node.self(), :core)

    send(
      Process.whereis(ServiceRegistry),
      {:nodedown, Node.self(), [nodedown_reason: :net_kernel_terminated]}
    )

    # Drain the mailbox past the nodedown message before asserting.
    :sys.get_state(ServiceRegistry)

    assert {:ok, _} = ServiceRegistry.get(Node.self(), :core),
           "the local core service must survive a spurious self-nodedown"
  end

  test "list/0 stamps :draining on services whose node is draining" do
    register_service!(ServiceInfo.new(Node.self(), :core))
    :ok = NodeRegistry.set_status(Node.self(), :draining)

    entry =
      ServiceRegistry.list()
      |> Enum.find(&(&1.node == Node.self() and &1.type == :core))

    assert entry.status == :draining
  end

  describe "a failed Ra write" do
    setup do
      Mimic.allow(RaSupervisor, self(), Process.whereis(ServiceRegistry))
      :ok
    end

    test "register/1 reports it to the caller instead of replying :ok" do
      stub(RaSupervisor, :command, fn _cmd, _timeout -> {:timeout, Node.self()} end)

      assert {:error, :timeout} =
               ServiceRegistry.register(ServiceInfo.new(:unreplicated@localhost, :nfs))
    end

    test "deregister/2 reports it to the caller instead of replying :ok" do
      stub(RaSupervisor, :command, fn _cmd, _timeout -> {:error, :ra_unavailable} end)

      assert {:error, :ra_unavailable} =
               ServiceRegistry.deregister(:unreplicated@localhost, :nfs)
    end

    # `:ra.process_command/3` answers `{:ok, Reply, Leader}` once the command
    # commits, whatever `Reply` is — so a state-machine rejection arrives under
    # an outer `:ok` and matching only that tag reports a phantom success.
    test "register/1 reports a state-machine rejection nested inside an :ok" do
      stub(RaSupervisor, :command, fn _cmd, _timeout ->
        {:ok, {:error, :rejected_by_machine}, Node.self()}
      end)

      assert {:error, :rejected_by_machine} =
               ServiceRegistry.register(ServiceInfo.new(:unreplicated@localhost, :nfs))
    end

    # The counterpart to the four above: those assert that a caller *is* told
    # the write failed, this asserts the test suite's own setup does not treat
    # that as fatal. The 500ms `@ra_write_timeout_ms` is documented to report
    # a failure for a write Ra later commits, so a hard `:ok =` in setup fails
    # on a loaded runner for a reason unrelated to the test.
    test "register_service!/1 rides out a transient failure" do
      {:ok, attempts} = Agent.start_link(fn -> 0 end)

      stub(RaSupervisor, :command, fn cmd, timeout ->
        case Agent.get_and_update(attempts, &{&1, &1 + 1}) do
          0 -> {:timeout, Node.self()}
          _ -> Mimic.call_original(RaSupervisor, :command, [cmd, timeout])
        end
      end)

      assert :ok = register_service!(ServiceInfo.new(:retried@localhost, :nfs))
      assert {:ok, _} = ServiceRegistry.get(:retried@localhost, :nfs)
      assert Agent.get(attempts, & &1) > 1, "the first attempt must actually have failed"
    end

    # The self-registration counterpart to the above. `register_service!/1`
    # only covers registrations a test issues itself; the endpoint-retry chain
    # registers on its own timer, so a setup waiting on telemetry needs the
    # same tolerance and had none.
    #
    # Three failed writes back off `500 + 1000 + 2000`ms, so the registration
    # lands well over the 1s this passes in. That is the whole point: the
    # argument bounds how long the chain may go *quiet*, not how long it may
    # take. Swap the call below for `assert_receive ..., slack_ms` and this
    # fails with "no matching message after 1000ms" — the exact shape of the
    # CI failure it guards. The elapsed-time assertion keeps it honest if the
    # backoff constants shrink.
    test "await_self_registration!/3 outlasts a backoff longer than its slack" do
      registry = Process.whereis(ServiceRegistry)
      for mod <- [Listener, PoolManager], do: Mimic.allow(mod, self(), registry)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :service_registry, :self_registered],
          [:neonfs, :service_registry, :self_registration_failed],
          [:neonfs, :service_registry, :self_register_retry_scheduled]
        ])

      listener = spawn(fn -> receive do: (:stop -> :ok) end)
      Process.register(listener, Listener)
      on_exit(fn -> Process.exit(listener, :kill) end)

      stub(Listener, :get_port, fn -> 4001 end)
      stub(PoolManager, :advertise_endpoint, fn port -> "127.0.0.1:#{port}" end)

      {:ok, attempts} = Agent.start_link(fn -> 0 end)

      stub(RaSupervisor, :command, fn
        {:register_service, _info} = cmd, timeout ->
          case Agent.get_and_update(attempts, &{&1, &1 + 1}) do
            n when n < 3 -> {:timeout, Node.self()}
            _ -> Mimic.call_original(RaSupervisor, :command, [cmd, timeout])
          end

        cmd, timeout ->
          Mimic.call_original(RaSupervisor, :command, [cmd, timeout])
      end)

      slack_ms = 1_000
      started_at = System.monotonic_time(:millisecond)

      ServiceRegistry.refresh_self()

      assert :ok = await_self_registration!(ref, "127.0.0.1:4001", slack_ms)

      elapsed_ms = System.monotonic_time(:millisecond) - started_at

      assert Agent.get(attempts, & &1) > 3, "the first three writes must actually have failed"

      assert elapsed_ms > slack_ms,
             "the backoff must outlast the slack, or this passes without exercising it"
    end

    test "register_service!/1 still raises when the write never lands" do
      stub(RaSupervisor, :command, fn _cmd, _timeout -> {:timeout, Node.self()} end)

      assert_raise RuntimeError, ~r/kept failing/, fn ->
        register_service!(ServiceInfo.new(:doomed@localhost, :nfs), timeout: 100)
      end
    end

    test "the registry survives it and keeps answering" do
      stub(RaSupervisor, :command, fn _cmd, _timeout -> {:timeout, Node.self()} end)

      assert {:error, :timeout} =
               ServiceRegistry.register(ServiceInfo.new(:unreplicated@localhost, :nfs))

      assert Process.alive?(Process.whereis(ServiceRegistry))
      assert is_list(ServiceRegistry.list())
    end
  end

  describe "self-registration whose Ra write fails" do
    setup do
      registry = Process.whereis(ServiceRegistry)

      for mod <- [RaSupervisor, Listener, PoolManager], do: Mimic.allow(mod, self(), registry)

      # Attach before the stand-in goes up, not after. The event waited for
      # below is the one that *ends* the chain, so it is emitted exactly once —
      # an attach that lands after the endpoint already exists misses it and
      # then waits out its timeout for a second one that never comes.
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :service_registry, :self_registered],
          [:neonfs, :service_registry, :self_registration_failed],
          [:neonfs, :service_registry, :self_register_retry_scheduled]
        ])

      # `build_self_metadata/0` gates on a live `Listener` before it asks for a
      # port, so without a stand-in every attempt looks like the endpoint case
      # and masks the write case this exercises.
      listener = spawn(fn -> receive do: (:stop -> :ok) end)
      Process.register(listener, Listener)
      on_exit(fn -> Process.exit(listener, :kill) end)

      stub(Listener, :get_port, fn -> 4001 end)
      stub(PoolManager, :advertise_endpoint, fn port -> "127.0.0.1:#{port}" end)

      # The boot registration found no `Listener` and left an endpoint-retry
      # chain ticking once a second. Now that the stand-in is up, its next tick
      # registers with an endpoint and the chain stops. Wait for that, then drop
      # what it emitted on the way — `assert_receive` matches selectively, so a
      # leftover would satisfy an assertion below without the test having caused
      # it.
      await_self_registration!(ref, "127.0.0.1:4001")

      flush_telemetry(ref)

      %{ref: ref}
    end

    test "is retried until it commits, and the node ends up registered", %{ref: ref} do
      :ok = ServiceRegistry.deregister(Node.self(), :core)
      {:ok, attempts} = Agent.start_link(fn -> 0 end)

      # Fail the first `:register_service` only, then hand every later command
      # to the real Ra write — so a passing test means the retry genuinely
      # landed in the state machine, not just that it was attempted.
      stub(RaSupervisor, :command, fn
        {:register_service, _info} = cmd, timeout ->
          case Agent.get_and_update(attempts, &{&1, &1 + 1}) do
            0 -> {:timeout, Node.self()}
            _ -> Mimic.call_original(RaSupervisor, :command, [cmd, timeout])
          end

        cmd, timeout ->
          Mimic.call_original(RaSupervisor, :command, [cmd, timeout])
      end)

      ServiceRegistry.refresh_self()

      assert_receive {[:neonfs, :service_registry, :self_registration_failed], ^ref,
                      %{attempt: 0}, %{reason: :timeout}},
                     1_000

      assert_receive {[:neonfs, :service_registry, :self_register_retry_scheduled], ^ref,
                      %{attempt: 0}, %{cause: :write}},
                     1_000

      assert_receive {[:neonfs, :service_registry, :self_registered], ^ref, %{attempt: 1}, _meta},
                     5_000

      assert {:ok, info} = ServiceRegistry.get(Node.self(), :core)
      assert info.metadata.data_endpoint == "127.0.0.1:4001"
    end

    test "is reported as a write retry, never as an endpoint one", %{ref: ref} do
      stub(RaSupervisor, :command, fn _cmd, _timeout -> {:timeout, Node.self()} end)

      ServiceRegistry.refresh_self()

      assert_receive {[:neonfs, :service_registry, :self_register_retry_scheduled], ^ref, _m,
                      %{cause: cause}},
                     1_000

      assert cause == :write,
             "a registration that committed without an endpoint and one that never " <>
               "committed need different retry budgets, so they must not share a cause"
    end
  end

  describe "a core service deregistered by someone else" do
    setup do
      registry = Process.whereis(ServiceRegistry)

      for mod <- [Listener, PoolManager], do: Mimic.allow(mod, self(), registry)

      # Attach before the endpoint exists, not after. The event asserted below
      # fires exactly once — a poll that finds an endpoint registers, and a
      # registration that succeeds ends the chain — so an attach that lands
      # after the stubs can miss it outright and then wait for a second event
      # that is never coming.
      # The retry events are attached for `await_self_registration!/3` below,
      # which reads the chain's announced delay to know how long to wait. The
      # assertions in this block match selectively, so the extra traffic is
      # inert to them.
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :service_registry, :self_registered],
          [:neonfs, :service_registry, :self_registration_healed],
          [:neonfs, :service_registry, :self_registration_failed],
          [:neonfs, :service_registry, :self_register_retry_scheduled]
        ])

      # Without an endpoint the boot registration leaves a retry chain running,
      # and the self-heal defers to it. Give it one so the chain terminates and
      # the heal is the only thing acting.
      listener = spawn(fn -> receive do: (:stop -> :ok) end)
      Process.register(listener, Listener)
      on_exit(fn -> Process.exit(listener, :kill) end)

      stub(Listener, :get_port, fn -> 4001 end)
      stub(PoolManager, :advertise_endpoint, fn port -> "127.0.0.1:#{port}" end)

      await_self_registration!(ref, "127.0.0.1:4001")

      # Shorten the interval and tick once, so the chain the assertions wait on
      # reschedules itself at the test cadence. The heal under test still comes
      # from a real timer, not from a `send/2` in the test body.
      Application.put_env(:neonfs_core, :service_registry_self_heal_interval, 100)

      on_exit(fn ->
        Application.delete_env(:neonfs_core, :service_registry_self_heal_interval)
      end)

      send(registry, :self_heal)

      flush_telemetry(ref)

      %{ref: ref}
    end

    test "re-registers itself, the way a peer's nodedown leaves it", %{ref: ref} do
      # What a peer does on `{:nodedown, this_node}`: drop every service the
      # node had. The node itself has no reason to notice — its own last
      # registration succeeded.
      :ok = ServiceRegistry.deregister(Node.self(), :core)
      assert {:error, :not_found} = ServiceRegistry.get(Node.self(), :core)

      assert_receive {[:neonfs, :service_registry, :self_registration_healed], ^ref, _m, _meta},
                     5_000

      # The heal event announces the decision; the registration it drives lands
      # after it, so the read has to wait for that instead.
      assert_receive {[:neonfs, :service_registry, :self_registered], ^ref, _m2, _meta2}, 5_000

      assert {:ok, info} = ServiceRegistry.get(Node.self(), :core)
      assert info.metadata.data_endpoint == "127.0.0.1:4001"
    end

    test "is left alone while a self-registration retry is already in flight", %{ref: ref} do
      registry = Process.whereis(ServiceRegistry)
      Mimic.allow(RaSupervisor, self(), registry)
      stub(RaSupervisor, :command, fn _cmd, _timeout -> {:timeout, Node.self()} end)

      # Put a retry chain in flight, then drive the heal directly. Each heal
      # that acted here would start another chain, and the node stays absent
      # while they run, so the timers would compound every interval.
      ServiceRegistry.refresh_self()
      assert %{retry_pending?: true} = :sys.get_state(registry)

      send(registry, :self_heal)
      :sys.get_state(registry)

      refute_received {[:neonfs, :service_registry, :self_registration_healed], ^ref, _m, _meta}
    end
  end

  # The endpoint chain used to stop after 60 tries, leaving the node
  # registered but with no `:data_endpoint` for the rest of its uptime — no
  # client reports it unreachable, and no peer can open a data-plane pool to
  # it.
  describe "the data-plane endpoint retry" do
    test "keeps polling well past the old 60-attempt bound" do
      registry = Process.whereis(ServiceRegistry)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :service_registry, :self_register_retry_scheduled]
        ])

      send(registry, {:retry_register_self, :endpoint, 500})

      assert_receive {[:neonfs, :service_registry, :self_register_retry_scheduled], ^ref,
                      %{attempt: 500}, %{cause: :endpoint}},
                     2_000
    end

    # Retrying forever is only affordable because a tick that finds no
    # endpoint issues no Ra command. The old chain wrote on every tick.
    test "a poll that finds no endpoint issues no Ra write" do
      registry = Process.whereis(ServiceRegistry)
      Mimic.allow(RaSupervisor, self(), registry)

      await_self_registration_landed!()

      {:ok, writes} = Agent.start_link(fn -> 0 end)

      stub(RaSupervisor, :command, fn
        {:register_service, _info} = cmd, timeout ->
          Agent.update(writes, &(&1 + 1))
          Mimic.call_original(RaSupervisor, :command, [cmd, timeout])

        cmd, timeout ->
          Mimic.call_original(RaSupervisor, :command, [cmd, timeout])
      end)

      # Count from what the process has already queued rather than from zero,
      # so the number belongs to the poll. A `:self_heal` tick can write too,
      # on its own timer, for a reason this test is not about.
      :sys.get_state(registry)
      before_poll = Agent.get(writes, & &1)

      send(registry, {:retry_register_self, :endpoint, 1})
      :sys.get_state(registry)

      assert Agent.get(writes, & &1) == before_poll,
             "nothing has changed since the last registration, so there is nothing to say"
    end

    # An endpoint tick no longer reissues the write, so a node whose write
    # failed *and* whose Listener is slow must still retry the write — else it
    # polls forever while unregistered.
    test "a failed write is retried even while the endpoint is still missing" do
      registry = Process.whereis(ServiceRegistry)
      Mimic.allow(RaSupervisor, self(), registry)
      stub(RaSupervisor, :command, fn _cmd, _timeout -> {:timeout, Node.self()} end)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :service_registry, :self_register_retry_scheduled]
        ])

      ServiceRegistry.refresh_self()

      assert_receive {[:neonfs, :service_registry, :self_register_retry_scheduled], ^ref, _m,
                      %{cause: :write}},
                     2_000
    end
  end

  # The `Listener` is not part of this suite's fixture, so the registry booted
  # without a data-plane endpoint and the chain it started is already running.
  test "a self-registration with no data-plane endpoint retries for the endpoint" do
    refute Process.whereis(Listener),
           "this test needs `build_self_metadata/0` to yield no endpoint"

    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:neonfs, :service_registry, :self_register_retry_scheduled]
      ])

    assert_receive {[:neonfs, :service_registry, :self_register_retry_scheduled], ^ref, _m,
                    %{cause: :endpoint}},
                   2_000
  end

  test "list/0 stamps :maintenance on services whose node is cordoned" do
    register_service!(ServiceInfo.new(Node.self(), :core))
    :ok = NodeRegistry.set_status(Node.self(), :maintenance)

    entry =
      ServiceRegistry.list()
      |> Enum.find(&(&1.node == Node.self() and &1.type == :core))

    assert entry.status == :maintenance
  end

  # A write that misses the 500ms `@ra_write_timeout_ms` is reported as failed
  # even when Ra commits it moments later, so on a loaded runner the
  # registration waited for here can be preceded by any number of write-cause
  # retries, each backing off further than the last. No fixed window bounds
  # that — but the chain announces its own next tick, so wait for what it said
  # plus slack and fail only once it has gone quiet for longer than that.
  # `register_service!/1` rides out the same deadline for an explicit
  # registration; this is the self-registration counterpart.
  defp await_self_registration!(ref, endpoint, slack_ms \\ @registration_quiet_ms) do
    do_await_self_registration!(ref, endpoint, slack_ms, slack_ms)
  end

  defp do_await_self_registration!(ref, endpoint, slack_ms, wait_ms) do
    receive do
      {[:neonfs, :service_registry, :self_registered], ^ref, _measurements,
       %{data_endpoint: ^endpoint}} ->
        :ok

      {[:neonfs, :service_registry, :self_register_retry_scheduled], ^ref, %{delay_ms: delay_ms},
       _metadata} ->
        do_await_self_registration!(ref, endpoint, slack_ms, delay_ms + slack_ms)

      {_event, ^ref, _measurements, _metadata} ->
        do_await_self_registration!(ref, endpoint, slack_ms, wait_ms)
    after
      wait_ms ->
        flunk(
          "the self-registration chain never registered #{endpoint} and has been " <>
            "quiet for #{wait_ms}ms"
        )
    end
  end

  # Blocks until the registry's own boot registration has committed.
  #
  # Until it has, a write-cause retry chain is ticking, and every tick
  # reissues the write — so a test counting Ra writes attributes one of those
  # ticks to whatever it was doing instead. A committed registration schedules
  # no further write tick, which is what makes the count attributable.
  #
  # Attach before reading the registry, never after: an event that fired in
  # between is then already in the mailbox, and a read that says "absent" is
  # therefore a genuine "has not committed yet" rather than a missed event.
  defp await_self_registration_landed! do
    ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:neonfs, :service_registry, :self_registered]
      ])

    unless match?({:ok, _info}, ServiceRegistry.get(Node.self(), :core)) do
      assert_receive {[:neonfs, :service_registry, :self_registered], ^ref, _measurements,
                      _metadata},
                     @registration_quiet_ms * 2
    end

    flush_telemetry(ref)
  end

  defp flush_telemetry(ref) do
    receive do
      {_event, ^ref, _measurements, _metadata} -> flush_telemetry(ref)
    after
      0 -> :ok
    end
  end

  defp start_test_peer(name, dist_port, peer_ports_env) do
    code_paths =
      :code.get_path()
      |> Enum.flat_map(fn path -> [~c"-pa", path] end)

    args =
      [~c"-start_epmd", ~c"false", ~c"-epmd_module", ~c"Elixir.NeonFS.Epmd"] ++ code_paths

    env = [
      {~c"NEONFS_DIST_PORT", Integer.to_charlist(dist_port)},
      {~c"NEONFS_PEER_PORTS", String.to_charlist(peer_ports_env)}
    ]

    peer_opts = %{
      name: name,
      host: ~c"localhost",
      args: args,
      env: env,
      # Standard I/O rather than a TCP control port: with a port the origin
      # listens and waits for the peer to dial back, and OTP hard-codes that
      # accept at 60s (`ACCEPT_TIMEOUT` in `peer.erl`). A saturated runner
      # that overruns it burns a minute and then fails boot with
      # `{:inet_async, :timeout}`, which no retry budget can avoid because
      # backoff only lengthens the gaps between attempts. Standard I/O opens
      # no listen socket, so the ceiling does not exist, and peer lifetime is
      # tied to the origin's port so an abandoned peer cannot outlive the run.
      connection: :standard_io,
      wait_boot: 60_000
    }

    {:ok, peer, node} = start_peer_with_retry(peer_opts, @peer_boot_attempts)

    {peer, node}
  end

  # `:peer.start/1` rather than `start_link/1`: a linked peer dying during
  # bring-up propagates an EXIT to the test process and fails the whole
  # module. `safe_stop_peer/1` in `on_exit` still bounds peer lifetime.
  defp start_peer_with_retry(peer_opts, attempts_left) do
    case :peer.start(peer_opts) do
      {:ok, peer, node} ->
        {:ok, peer, node}

      {:error, reason} = error ->
        if retry_boot?(attempts_left, reason) do
          backoff_and_retry_boot(peer_opts, attempts_left, reason)
        else
          error
        end
    end
  catch
    # `:peer.start/1` exits rather than returning `{:error, _}` for every boot
    # failure that is not an argument error: `wait_boot` expiring exits
    # `:timeout`, and a peer that dies mid-boot exits `{:boot_failed, reason}`.
    :exit, reason ->
      if retry_boot?(attempts_left, reason) do
        backoff_and_retry_boot(peer_opts, attempts_left, reason)
      else
        :erlang.raise(:exit, reason, __STACKTRACE__)
      end
  end

  defp retry_boot?(attempts_left, reason),
    do: attempts_left > 1 and transient_boot_error?(reason)

  defp backoff_and_retry_boot(peer_opts, attempts_left, reason) do
    Logger.warning(
      "peer boot failed transiently (#{inspect(reason)}), " <>
        "retrying (#{attempts_left - 1} attempt(s) left)"
    )

    Process.sleep(boot_backoff_ms(attempts_left))
    start_peer_with_retry(peer_opts, attempts_left - 1)
  end

  # Widen the wait as attempts are consumed so a runner under sustained load
  # near the end of a long run gets progressively more breathing room before
  # the suite gives up, capped so a stuck boot doesn't stall setup for long.
  # Without the cap, the eighth attempt would sleep `250 × 2⁶` = 16 s on its
  # own.
  defp boot_backoff_ms(attempts_left) do
    retries_used = @peer_boot_attempts - attempts_left
    min(@peer_boot_backoff_ms * Integer.pow(2, retries_used), @peer_boot_max_backoff_ms)
  end

  # The original of this classifier is in `NeonFS.TestSupport.PeerCluster`.
  # Calling it is not an option: `neonfs_test_support` depends on this package,
  # so the arrow does not run this way, and this file needs the same peer-boot
  # hardening `PeerCluster` grew. A new transient reason therefore has to be
  # added in both places — it has been revised twice already, and a revision
  # that lands in only one surfaces as a peer-boot flake in whichever package
  # was not updated.
  #
  # The retry *budget* above has to stay in step too — attempts, initial
  # backoff and the cap. Naming only the classifier is how this copy came to
  # be taken from an already-widened shape without carrying the widening.
  defp transient_boot_error?({:boot_failed, reason}), do: transient_boot_error?(reason)
  defp transient_boot_error?(:timeout), do: true
  defp transient_boot_error?(:tcp_closed), do: true
  defp transient_boot_error?({:inet_async, :timeout}), do: true
  defp transient_boot_error?(_reason), do: false

  defp allocate_port do
    {:ok, socket} = :gen_tcp.listen(0, reuseaddr: true)
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp safe_stop_peer(peer) do
    :peer.stop(peer)
  catch
    :exit, _ -> :ok
  end
end
