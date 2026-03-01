defmodule NeonFS.Cluster.Formation do
  @moduledoc """
  One-shot GenServer that implements autonomous cluster formation.

  Implements the Consul/Nomad `bootstrap_expect` pattern: all nodes get the
  same config, discover each other, wait for quorum, then deterministically
  elect an initialiser. The feature is opt-in via `NEONFS_AUTO_BOOTSTRAP=true`.

  ## State machine phases

      :checking_preconditions → :connecting_peers → :waiting_for_quorum
        → :forming_cluster | :joining_cluster → :complete

  ## Options

    * `:cluster_name` — name for the new cluster (required)
    * `:bootstrap_expect` — number of core nodes to wait for (required)
    * `:bootstrap_peers` — list of peer node atoms (required)
    * `:bootstrap_timeout` — max wait time in ms (default: 300_000)
  """

  use GenServer

  require Logger

  alias NeonFS.Cluster.{Init, Invite, Join, State}
  alias NeonFS.Core.DriveRegistry
  alias NeonFS.Core.Supervisor, as: CoreSupervisor

  @pg_scope :neonfs_events

  @ra_cluster_name :neonfs_meta
  @connect_interval_ms 500
  @readiness_check_interval_ms 250

  @type t :: %__MODULE__{
          bootstrap_expect: pos_integer(),
          bootstrap_peers: [node()],
          bootstrap_timeout: pos_integer(),
          cluster_name: String.t(),
          connected_peers: MapSet.t(node()),
          deadline: integer(),
          init_node: node() | nil,
          pg_monitor_ref: reference() | nil,
          phase: atom(),
          ready_peers: MapSet.t(node())
        }

  defstruct [
    :cluster_name,
    :bootstrap_expect,
    :bootstrap_peers,
    :bootstrap_timeout,
    :deadline,
    :pg_monitor_ref,
    :init_node,
    phase: :checking_preconditions,
    connected_peers: MapSet.new(),
    ready_peers: MapSet.new()
  ]

  # ── Client API ──────────────────────────────────────────────────

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Detects orphaned data: Ra WAL, DETS files, or blob data present
  without a `cluster.json`. Public for testability.
  """
  @spec orphaned_data_detected?() :: boolean()
  def orphaned_data_detected? do
    ra = has_ra_data?()
    dets = has_dets_files?()
    blob = has_blob_data?()

    if ra or dets or blob do
      Logger.warning("Orphan detection triggered",
        has_ra_data: ra,
        has_dets_files: dets,
        has_blob_data: blob
      )
    end

    ra or dets or blob
  end

  # ── GenServer callbacks ─────────────────────────────────────────

  @impl true
  def init(opts) do
    state = %__MODULE__{
      cluster_name: Keyword.fetch!(opts, :cluster_name),
      bootstrap_expect: Keyword.fetch!(opts, :bootstrap_expect),
      bootstrap_peers: Keyword.fetch!(opts, :bootstrap_peers),
      bootstrap_timeout: Keyword.get(opts, :bootstrap_timeout, 300_000),
      deadline:
        System.monotonic_time(:millisecond) + Keyword.get(opts, :bootstrap_timeout, 300_000)
    }

    {:ok, state, {:continue, :check_preconditions}}
  end

  @impl true
  def handle_continue(:check_preconditions, state) do
    # Prefer the result cached by Application.start/2 (run before supervisor
    # children started, avoiding false positives from sibling artifacts).
    # Fall back to a live check when Formation is started directly (e.g. in
    # integration tests or manual GenServer.start).
    orphaned =
      case Application.fetch_env(:neonfs_core, :orphaned_data_at_startup) do
        {:ok, value} -> value
        :error -> orphaned_data_detected?()
      end

    cond do
      State.exists?() ->
        Logger.info("Cluster state already exists, skipping formation")
        {:stop, :normal, state}

      orphaned ->
        Logger.error(
          "Orphaned data detected without cluster.json — " <>
            "clean data directories or restore cluster.json before retrying"
        )

        {:stop, {:shutdown, :orphaned_data_detected}, state}

      true ->
        Logger.info("Auto-bootstrap: starting cluster formation",
          cluster_name: state.cluster_name,
          bootstrap_expect: state.bootstrap_expect,
          peers: state.bootstrap_peers
        )

        send(self(), :connect_peers)
        {:noreply, %{state | phase: :connecting_peers}}
    end
  end

  # ── Phase 2: Connecting Peers ───────────────────────────────────

  @impl true
  def handle_info(:connect_peers, %{phase: :connecting_peers} = state) do
    cond do
      past_deadline?(state) -> timeout_stop("peers to connect", state)
      State.exists?() -> cluster_appeared_stop("peer connection", state)
      true -> do_connect_peers(state)
    end
  end

  # ── Phase 3: Waiting for Quorum ─────────────────────────────────

  def handle_info(:check_readiness, %{phase: :waiting_for_quorum} = state) do
    cond do
      past_deadline?(state) -> timeout_stop("node readiness", state)
      State.exists?() -> cluster_appeared_stop("readiness", state)
      true -> do_check_readiness(state)
    end
  end

  # Handle :pg monitor join messages
  def handle_info({ref, :join, {:node, :ready}, pids}, %{pg_monitor_ref: ref} = state) do
    new_ready =
      pids
      |> Enum.map(&node/1)
      |> Enum.filter(&(&1 in state.bootstrap_peers or &1 == Node.self()))
      |> MapSet.new()

    state = %{state | ready_peers: MapSet.union(state.ready_peers, new_ready)}

    # If we're in the waiting_for_quorum phase, check if we can advance
    if state.phase == :waiting_for_quorum do
      ready_count = MapSet.size(state.ready_peers)

      if ready_count >= state.bootstrap_expect do
        Logger.info("Auto-bootstrap: all nodes ready via pg join event",
          ready: ready_count,
          expected: state.bootstrap_expect
        )

        send(self(), :form_cluster)
        {:noreply, %{state | phase: :forming_cluster}}
      else
        {:noreply, state}
      end
    else
      {:noreply, state}
    end
  end

  # Ignore :pg leave messages and other monitor events
  def handle_info({ref, :leave, {:node, :ready}, _pids}, %{pg_monitor_ref: ref} = state) do
    {:noreply, state}
  end

  # ── Phase 4: Cluster Formation ──────────────────────────────────

  def handle_info(:form_cluster, %{phase: :forming_cluster} = state) do
    # Re-check: another node might have formed the cluster during the race
    if State.exists?() do
      Logger.info("Cluster state appeared before formation — another node won the race")
      finish_formation(state)
    else
      init_node = elect_init_node(state)
      state = %{state | init_node: init_node}

      if init_node == Node.self() do
        run_init_path(state)
      else
        Logger.info("Auto-bootstrap: waiting for invite from init node",
          init_node: init_node
        )

        # Set a shorter deadline for receiving the invite
        {:noreply, %{state | phase: :joining_cluster}}
      end
    end
  end

  # Join path: received invite from init node
  def handle_info(
        {:formation_invite, token, from_node},
        %{phase: :joining_cluster} = state
      ) do
    Logger.info("Auto-bootstrap: received invite from init node", init_node: from_node)

    case Join.join_cluster(token, from_node) do
      {:ok, _state} ->
        Logger.info("Auto-bootstrap: successfully joined cluster")
        send({__MODULE__, from_node}, {:join_complete, Node.self()})
        finish_formation(state)

      {:error, :already_in_cluster} ->
        Logger.info("Auto-bootstrap: already in cluster")
        send({__MODULE__, from_node}, {:join_complete, Node.self()})
        finish_formation(state)

      {:error, reason} ->
        Logger.error("Auto-bootstrap: failed to join cluster", reason: inspect(reason))
        {:stop, {:shutdown, {:join_failed, reason}}, state}
    end
  end

  # Init path: received join confirmation from a peer
  def handle_info(
        {:join_complete, peer},
        %{phase: :forming_cluster} = state
      ) do
    Logger.info("Auto-bootstrap: peer joined successfully", peer: peer)
    # We continue regardless — joins are best-effort during formation
    {:noreply, state}
  end

  # Catch-all for unexpected messages
  def handle_info(msg, state) do
    Logger.debug("Auto-bootstrap: ignoring unexpected message",
      message: inspect(msg),
      phase: state.phase
    )

    {:noreply, state}
  end

  @impl true
  def terminate(:normal, _state) do
    Logger.info("Auto-bootstrap: formation complete")
    :ok
  end

  def terminate({:shutdown, reason}, _state) do
    Logger.error("Auto-bootstrap: formation failed", reason: inspect(reason))
    :ok
  end

  def terminate(reason, _state) do
    Logger.error("Auto-bootstrap: formation crashed", reason: inspect(reason))
    :ok
  end

  # ── Private: phase guards ─────────────────────────────────────

  defp timeout_stop(waiting_for, state) do
    Logger.error("Auto-bootstrap: timed out waiting for #{waiting_for}")
    {:stop, {:shutdown, :bootstrap_timeout}, state}
  end

  defp cluster_appeared_stop(phase_label, state) do
    Logger.info("Cluster state appeared during #{phase_label} phase")
    {:stop, :normal, state}
  end

  defp do_connect_peers(state) do
    state = connect_to_peers(state)
    connected_count = 1 + MapSet.size(state.connected_peers)

    if connected_count >= state.bootstrap_expect do
      Logger.info("Auto-bootstrap: all peers connected, waiting for readiness",
        connected: connected_count,
        expected: state.bootstrap_expect
      )

      send(self(), :check_readiness)
      {:noreply, %{state | phase: :waiting_for_quorum}}
    else
      Logger.debug("Auto-bootstrap: waiting for peers",
        connected: connected_count,
        expected: state.bootstrap_expect
      )

      Process.send_after(self(), :connect_peers, @connect_interval_ms)
      {:noreply, state}
    end
  end

  defp do_check_readiness(state) do
    state = monitor_readiness(state)
    state = check_ready_peers(state)
    ready_count = MapSet.size(state.ready_peers)

    if ready_count >= state.bootstrap_expect do
      Logger.info("Auto-bootstrap: all nodes ready, forming cluster",
        ready: ready_count,
        expected: state.bootstrap_expect
      )

      send(self(), :form_cluster)
      {:noreply, %{state | phase: :forming_cluster}}
    else
      Logger.debug("Auto-bootstrap: waiting for readiness",
        ready: ready_count,
        expected: state.bootstrap_expect
      )

      Process.send_after(self(), :check_readiness, @readiness_check_interval_ms)
      {:noreply, state}
    end
  end

  # ── Private: peer connection ────────────────────────────────────

  defp connect_to_peers(state) do
    newly_connected =
      state.bootstrap_peers
      |> Enum.reject(&(&1 == Node.self() or MapSet.member?(state.connected_peers, &1)))
      |> Enum.filter(&(Node.connect(&1) == true))
      |> MapSet.new()

    %{state | connected_peers: MapSet.union(state.connected_peers, newly_connected)}
  end

  # ── Private: readiness monitoring ───────────────────────────────

  defp monitor_readiness(%{pg_monitor_ref: nil} = state) do
    {ref, _current_members} = :pg.monitor(@pg_scope, {:node, :ready})
    %{state | pg_monitor_ref: ref}
  end

  defp monitor_readiness(state), do: state

  defp check_ready_peers(state) do
    # Check current :pg members for bootstrap peers
    current_members =
      try do
        :pg.get_members(@pg_scope, {:node, :ready})
      catch
        _, _ -> []
      end

    ready_nodes =
      current_members
      |> Enum.map(&node/1)
      |> Enum.filter(&(&1 in state.bootstrap_peers or &1 == Node.self()))
      |> MapSet.new()

    %{state | ready_peers: MapSet.union(state.ready_peers, ready_nodes)}
  end

  # ── Private: leader election ────────────────────────────────────

  @doc false
  # Visible for testing — deterministic election based on lexicographic sort
  def elect_init_node(state) do
    all_nodes = [Node.self() | state.bootstrap_peers] |> Enum.uniq() |> Enum.sort()
    List.first(all_nodes)
  end

  # ── Private: init path ──────────────────────────────────────────

  defp run_init_path(state) do
    Logger.info("Auto-bootstrap: this node is the init node, initialising cluster",
      cluster_name: state.cluster_name
    )

    case Init.init_cluster(state.cluster_name) do
      {:ok, cluster_id} ->
        Logger.info("Auto-bootstrap: cluster initialised", cluster_id: cluster_id)
        distribute_invites_sequentially(state)
        finish_formation(state)

      {:error, :already_initialised} ->
        Logger.info("Auto-bootstrap: cluster already initialised")
        distribute_invites_sequentially(state)
        finish_formation(state)

      {:error, reason} ->
        Logger.error("Auto-bootstrap: init_cluster failed", reason: inspect(reason))
        {:stop, {:shutdown, {:init_failed, reason}}, state}
    end
  end

  # Distribute invites one peer at a time, waiting for each join to
  # complete before sending the next.  This avoids concurrent Ra
  # membership changes which cause `:cluster_change_not_permitted`
  # contention and long retry loops.
  #
  # The `receive` is safe here: `accept_join` runs in an RPC handler
  # process (not this GenServer), so it executes while we wait.
  defp distribute_invites_sequentially(state) do
    peers = Enum.reject(state.bootstrap_peers, &(&1 == Node.self()))

    case Invite.create_invite(3600) do
      {:ok, token} ->
        Logger.info("Auto-bootstrap: distributing invites to peers", peer_count: length(peers))

        Enum.each(peers, fn peer ->
          send({__MODULE__, peer}, {:formation_invite, token, Node.self()})

          receive do
            {:join_complete, ^peer} ->
              Logger.info("Auto-bootstrap: peer join confirmed", peer: peer)

              # Wait for the peer's Ra server to catch up before inviting the
              # next peer.  Without this, the next `accept_join` → `add_member`
              # hits `:cluster_change_not_permitted` while Ra replicates to the
              # just-added member.
              wait_for_ra_member_sync(peer)
          after
            60_000 ->
              Logger.warning("Auto-bootstrap: timeout waiting for peer join", peer: peer)
          end
        end)

      {:error, reason} ->
        Logger.error("Auto-bootstrap: failed to create invite", reason: inspect(reason))
    end
  end

  # Poll until the peer's Ra server is an active member in the cluster.
  # This confirms the Ra membership change has fully committed (joint
  # consensus completed), so the next `add_member` won't be rejected.
  defp wait_for_ra_member_sync(peer, attempts \\ 0)

  defp wait_for_ra_member_sync(_peer, attempts) when attempts >= 30 do
    Logger.warning("Auto-bootstrap: timed out waiting for Ra member sync")
  end

  defp wait_for_ra_member_sync(peer, attempts) do
    server_id = {@ra_cluster_name, Node.self()}
    target = {@ra_cluster_name, peer}

    case :ra.members(server_id, 5_000) do
      {:ok, members, _leader} ->
        if target in members do
          :ok
        else
          receive do
          after
            200 -> :ok
          end

          wait_for_ra_member_sync(peer, attempts + 1)
        end

      _ ->
        receive do
        after
          200 -> :ok
        end

        wait_for_ra_member_sync(peer, attempts + 1)
    end
  end

  # ── Private: finish ─────────────────────────────────────────────

  defp finish_formation(state) do
    rebuild_quorum_rings(state)
    persist_drives()
    demonitor_pg(state)

    :telemetry.execute(
      [:neonfs, :cluster, :formation, :complete],
      %{
        duration: System.monotonic_time(:millisecond) - (state.deadline - state.bootstrap_timeout)
      },
      %{cluster_name: state.cluster_name, node: Node.self()}
    )

    {:stop, :normal, state}
  end

  defp rebuild_quorum_rings(state) do
    try do
      CoreSupervisor.rebuild_quorum_ring()
    catch
      _, reason ->
        Logger.warning("Auto-bootstrap: failed to rebuild local quorum ring",
          reason: inspect(reason)
        )
    end

    peers =
      state.bootstrap_peers
      |> Enum.reject(&(&1 == Node.self()))
      |> Enum.filter(&(&1 in Node.list()))

    for peer <- peers do
      try do
        :erpc.call(peer, CoreSupervisor, :rebuild_quorum_ring, [], 10_000)
      catch
        _, reason ->
          Logger.warning("Auto-bootstrap: failed to rebuild quorum ring on peer",
            peer: peer,
            reason: inspect(reason)
          )
      end
    end
  end

  defp persist_drives do
    drives = DriveRegistry.drives_for_node(Node.self())
    drive_configs = Enum.map(drives, &Map.take(&1, [:id, :path, :tier, :capacity]))
    State.update_drives(drive_configs)
  catch
    _, reason ->
      Logger.warning("Auto-bootstrap: failed to persist drives", reason: inspect(reason))
  end

  defp demonitor_pg(%{pg_monitor_ref: nil}), do: :ok

  defp demonitor_pg(%{pg_monitor_ref: ref}) do
    :pg.demonitor(@pg_scope, ref)
  end

  # ── Private: orphaned data detection ────────────────────────────
  #
  # These checks detect data from actual prior cluster operations,
  # not artifacts from fresh startup. BlobStore creates empty prefix
  # directories at startup; Persistence creates DETS files; Ra creates
  # WAL files. None of those indicate a prior cluster was active.
  #
  # The reliable markers of previous cluster activity are:
  # - Actual blob chunk files within the prefix directory tree
  # - Ra snapshot directories (created after consensus operations)
  # - DETS files with non-trivial size (> 1KB, vs ~0 bytes fresh)

  defp has_ra_data? do
    ra_dir =
      case Application.get_env(:ra, :data_dir) do
        nil -> nil
        dir when is_list(dir) -> to_string(dir)
        dir when is_binary(dir) -> dir
      end

    ra_dir != nil and File.exists?(ra_dir) and has_ra_snapshots?(ra_dir)
  end

  # Ra creates snapshot directories only after consensus operations
  # (elections, state machine commands). A fresh Ra server that was
  # started but never init'd/joined won't have snapshot directories.
  defp has_ra_snapshots?(ra_dir) do
    case File.ls(ra_dir) do
      {:ok, entries} ->
        Enum.any?(entries, fn entry ->
          subdir = Path.join(ra_dir, entry)

          File.dir?(subdir) and
            File.exists?(Path.join(subdir, "snapshots"))
        end)

      _ ->
        false
    end
  end

  defp has_dets_files? do
    meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/var/lib/neonfs/meta")

    with true <- File.exists?(meta_dir),
         {:ok, files} <- File.ls(meta_dir) do
      Enum.any?(files, &populated_dets_file?(meta_dir, &1))
    else
      _ -> false
    end
  end

  defp populated_dets_file?(dir, filename) do
    String.ends_with?(filename, ".dets") and
      dets_file_has_data?(Path.join(dir, filename))
  end

  defp dets_file_has_data?(path) do
    # Open the DETS file read-only and check for actual records rather than
    # relying on a file size threshold (empty DETS tables have ~5KB of header
    # overhead which varies by OTP version, making size checks unreliable).
    #
    # This is safe because orphan detection now runs in Application.start/2
    # before the supervisor starts, so no other process has these files open.
    tab = :"orphan_check_#{:erlang.unique_integer([:positive])}"

    case :dets.open_file(tab, file: String.to_charlist(path), access: :read) do
      {:ok, ref} ->
        count = :dets.info(ref, :size)
        :dets.close(ref)
        is_integer(count) and count > 0

      {:error, _} ->
        # File locked or corrupt — fall back to generous size threshold
        case File.stat(path) do
          {:ok, %{size: size}} -> size > 8192
          _ -> false
        end
    end
  end

  defp has_blob_data? do
    blob_dir = Application.get_env(:neonfs_core, :blob_store_base_dir, "/var/lib/neonfs/blobs")

    if File.exists?(blob_dir) do
      has_blob_files?(blob_dir)
    else
      false
    end
  end

  # Recursively check for actual files (not just directories) within
  # the blob directory tree. BlobStore creates prefix directories on
  # startup but only stores actual chunk files after write operations.
  defp has_blob_files?(dir) do
    case File.ls(dir) do
      {:ok, entries} -> Enum.any?(entries, &regular_or_nested_file?(Path.join(dir, &1)))
      _ -> false
    end
  end

  defp regular_or_nested_file?(path) do
    cond do
      File.regular?(path) -> true
      File.dir?(path) -> has_blob_files?(path)
      true -> false
    end
  end

  # ── Private: deadline ───────────────────────────────────────────

  defp past_deadline?(state) do
    System.monotonic_time(:millisecond) > state.deadline
  end
end
