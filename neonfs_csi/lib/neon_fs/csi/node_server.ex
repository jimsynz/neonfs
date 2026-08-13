defmodule NeonFS.CSI.NodeServer do
  @moduledoc """
  CSI v1 Node service implementation. Runs as a Kubernetes DaemonSet
  pod (one per worker node) and performs the actual mount lifecycle
  on the host so pods can read/write NeonFS volumes.

  The Identity service
  advertises this plugin to the kubelet via the per-node socket;
  the Controller service handles cluster-wide volume lifecycle.

  ## RPCs implemented

    * `NodeGetCapabilities` — advertises `STAGE_UNSTAGE_VOLUME`. The
      kubelet uses this to decide whether to call `NodeStageVolume`
      before `NodePublishVolume`.
    * `NodeGetInfo` — returns the node ID the Controller uses to
      track publish targets. Defaults to `Node.self/0` stringified
      so peer-cluster harnesses can identify each node, but operators
      override via the `NODE_ID` env var (CSI standard).
    * `NodeStageVolume` — drives the FUSE mount of the volume at the
      kubelet-supplied staging path. One staged mount per volume per
      node; subsequent stages of the same volume are idempotent.
    * `NodeUnstageVolume` — tears the FUSE mount down. Refuses to
      unstage if any pod is still publishing the volume.
    * `NodePublishVolume` — bind-mounts the staging path into the
      pod-specific target path with the requested access mode
      (rw / ro).
    * `NodeUnpublishVolume` — unmounts the bind mount.
    * `NodeGetVolumeStats` — reports per-mount usage (from the
      controller-side volume stats) and a `VolumeCondition` derived
      from a host-local probe of the staging path. A wedged FUSE mount
      surfaces as `abnormal = true` so kubelet can reschedule pods.

  ## Test injection

  Both the FUSE mount call and the host bind-mount syscall are
  routed through application-env hooks so unit tests can run on a
  developer laptop without `/dev/fuse` or `CAP_SYS_ADMIN`:

    * `:fuse_mount_fn` — `(volume_name, staging_path) -> {:ok, mount_id} | {:error, term()}`.
      Default `GenServer.call({MountManager, fuse_node}, {:mount, …})`
      against the FUSE node configured via `:fuse_node`.
    * `:fuse_unmount_fn` — `(mount_id) -> :ok | {:error, term()}`.
    * `:bind_mount_fn` — `(staging_path, target_path, ro?) -> :ok | {:error, term()}`.
      Default invokes `mount(8)` with `--bind` (and `-o remount,ro`
      when `readonly: true`).
    * `:bind_unmount_fn` — `(target_path) -> :ok | {:error, term()}`.

  ## State

  All state lives in two named ETS tables initialised by
  `init_state_tables/0` (called once from the supervisor):

    * `@staged_table` — `{volume_id} -> %{staging_path, mount_id}`.
      One row per volume per node, matching CSI's
      stage-once / unstage-once contract.
    * `@published_table` — `{volume_id, target_path} -> %{staging_path, readonly}`.
      One row per pod publish target.

  ETS lets the gRPC stub be stateless — every RPC is dispatched on
  its own gRPC handler process and they all read / write the same
  table.
  """

  use GRPC.Server, service: Csi.V1.Node.Service

  alias Csi.V1.{
    NodeGetCapabilitiesRequest,
    NodeGetCapabilitiesResponse,
    NodeGetInfoRequest,
    NodeGetInfoResponse,
    NodeGetVolumeStatsRequest,
    NodeGetVolumeStatsResponse,
    NodePublishVolumeRequest,
    NodePublishVolumeResponse,
    NodeServiceCapability,
    NodeStageVolumeRequest,
    NodeStageVolumeResponse,
    NodeUnpublishVolumeRequest,
    NodeUnpublishVolumeResponse,
    NodeUnstageVolumeRequest,
    NodeUnstageVolumeResponse,
    VolumeCondition,
    VolumeUsage
  }

  alias NeonFS.Client.Discovery
  alias NeonFS.CSI.VolumeHealth

  @staged_table :csi_node_staged
  @published_table :csi_node_published

  ## State table lifecycle

  @doc """
  Initialise the ETS tables backing staged and published volumes.
  Called once by the supervisor at boot; idempotent.
  """
  @spec init_state_tables() :: :ok
  def init_state_tables do
    if :ets.whereis(@staged_table) == :undefined do
      :ets.new(@staged_table, [:named_table, :set, :public, read_concurrency: true])
    end

    if :ets.whereis(@published_table) == :undefined do
      :ets.new(@published_table, [:named_table, :set, :public, read_concurrency: true])
    end

    :ok
  end

  @doc "Clears state tables. Test-only convenience."
  @spec reset_state_tables() :: :ok
  def reset_state_tables do
    init_state_tables()
    :ets.delete_all_objects(@staged_table)
    :ets.delete_all_objects(@published_table)
    :ok
  end

  ## RPCs

  @doc "CSI `Node.NodeGetCapabilities` — declares the supported RPCs."
  @spec node_get_capabilities(NodeGetCapabilitiesRequest.t(), term()) ::
          NodeGetCapabilitiesResponse.t()
  def node_get_capabilities(%NodeGetCapabilitiesRequest{}, _stream) do
    %NodeGetCapabilitiesResponse{
      capabilities:
        Enum.map(
          [:STAGE_UNSTAGE_VOLUME, :GET_VOLUME_STATS, :VOLUME_CONDITION],
          &capability/1
        )
    }
  end

  @doc """
  CSI `Node.NodeGetInfo` — returns the node ID the kubelet will hand
  to the Controller during `ControllerPublishVolume`. Defaults to the
  Erlang node name; operators can override with the `NODE_ID` env var
  to match Kubernetes node labels (the CSI sidecar standard).
  """
  @spec node_get_info(NodeGetInfoRequest.t(), term()) :: NodeGetInfoResponse.t()
  def node_get_info(%NodeGetInfoRequest{}, _stream) do
    %NodeGetInfoResponse{node_id: node_id()}
  end

  @doc """
  CSI `Node.NodeStageVolume` — mount the NeonFS volume at the
  staging path. Idempotent: a second stage of the same `volume_id`
  pointing at the same path returns success without remounting.
  """
  @spec node_stage_volume(NodeStageVolumeRequest.t(), term()) :: NodeStageVolumeResponse.t()
  def node_stage_volume(%NodeStageVolumeRequest{volume_id: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_id is required"
  end

  def node_stage_volume(%NodeStageVolumeRequest{staging_target_path: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "staging_target_path is required"
  end

  def node_stage_volume(
        %NodeStageVolumeRequest{volume_capability: nil},
        _stream
      ) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_capability is required"
  end

  def node_stage_volume(
        %NodeStageVolumeRequest{
          volume_id: vol_id,
          staging_target_path: staging_path,
          volume_capability: cap
        },
        _stream
      ) do
    init_state_tables()
    ensure_capability_supported!(cap)
    ensure_capability_matches_volume!(vol_id, cap)

    case :ets.lookup(@staged_table, vol_id) do
      [{^vol_id, %{staging_path: ^staging_path}}] ->
        %NodeStageVolumeResponse{}

      [{^vol_id, %{staging_path: existing}}] ->
        raise GRPC.RPCError,
          status: :failed_precondition,
          message:
            "volume #{vol_id} is already staged at #{existing} (cannot re-stage at #{staging_path})"

      [] ->
        do_stage(vol_id, staging_path, access_type(cap))
    end
  end

  @doc """
  CSI `Node.NodeUnstageVolume` — unmount the FUSE mount. Refuses to
  unstage while pods on the node still have publishes outstanding,
  matching the CSI invariant that Unpublish precedes Unstage.
  """
  @spec node_unstage_volume(NodeUnstageVolumeRequest.t(), term()) ::
          NodeUnstageVolumeResponse.t()
  def node_unstage_volume(%NodeUnstageVolumeRequest{volume_id: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_id is required"
  end

  def node_unstage_volume(%NodeUnstageVolumeRequest{staging_target_path: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "staging_target_path is required"
  end

  def node_unstage_volume(
        %NodeUnstageVolumeRequest{volume_id: vol_id, staging_target_path: staging_path},
        _stream
      ) do
    init_state_tables()

    case :ets.lookup(@staged_table, vol_id) do
      [] ->
        %NodeUnstageVolumeResponse{}

      [{^vol_id, %{staging_path: ^staging_path} = record}] ->
        if has_publishes?(vol_id) do
          raise GRPC.RPCError,
            status: :failed_precondition,
            message: "volume #{vol_id} still has active publishes; unpublish first"
        end

        do_unstage(vol_id, record)

      [{^vol_id, %{staging_path: existing}}] ->
        raise GRPC.RPCError,
          status: :failed_precondition,
          message: "volume #{vol_id} is staged at #{existing}, not #{staging_path}"
    end
  end

  @doc """
  CSI `Node.NodePublishVolume` — bind-mount the staging path into the
  pod-specific target path. Idempotent: a republish at the same
  target with the same mode returns success.
  """
  @spec node_publish_volume(NodePublishVolumeRequest.t(), term()) ::
          NodePublishVolumeResponse.t()
  def node_publish_volume(%NodePublishVolumeRequest{volume_id: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_id is required"
  end

  def node_publish_volume(%NodePublishVolumeRequest{target_path: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "target_path is required"
  end

  def node_publish_volume(%NodePublishVolumeRequest{staging_target_path: ""}, _stream) do
    raise GRPC.RPCError,
      status: :failed_precondition,
      message: "staging_target_path is required (volume must be staged first)"
  end

  def node_publish_volume(
        %NodePublishVolumeRequest{volume_capability: nil},
        _stream
      ) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_capability is required"
  end

  def node_publish_volume(
        %NodePublishVolumeRequest{
          volume_id: vol_id,
          target_path: target_path,
          staging_target_path: staging_path,
          volume_capability: cap,
          readonly: ro
        },
        _stream
      ) do
    init_state_tables()
    ensure_capability_supported!(cap)
    ensure_capability_matches_staged!(vol_id, cap)
    ensure_staged!(vol_id, staging_path)

    case :ets.lookup(@published_table, {vol_id, target_path}) do
      [{_, %{readonly: ^ro}}] ->
        %NodePublishVolumeResponse{}

      [{_, %{readonly: existing_ro}}] ->
        raise GRPC.RPCError,
          status: :already_exists,
          message:
            "target #{target_path} already published with readonly=#{existing_ro}, cannot remount with readonly=#{ro}"

      [] ->
        do_publish(vol_id, staging_path, target_path, ro)
    end
  end

  @doc """
  CSI `Node.NodeUnpublishVolume` — tear down the bind mount and drop
  the published-target row. Idempotent for unknown targets.
  """
  @spec node_unpublish_volume(NodeUnpublishVolumeRequest.t(), term()) ::
          NodeUnpublishVolumeResponse.t()
  def node_unpublish_volume(%NodeUnpublishVolumeRequest{volume_id: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_id is required"
  end

  def node_unpublish_volume(%NodeUnpublishVolumeRequest{target_path: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "target_path is required"
  end

  def node_unpublish_volume(
        %NodeUnpublishVolumeRequest{volume_id: vol_id, target_path: target_path},
        _stream
      ) do
    init_state_tables()

    case :ets.lookup(@published_table, {vol_id, target_path}) do
      [] ->
        %NodeUnpublishVolumeResponse{}

      [{_, _record}] ->
        case bind_unmount_fn().(target_path) do
          :ok ->
            :ets.delete(@published_table, {vol_id, target_path})
            %NodeUnpublishVolumeResponse{}

          {:error, reason} ->
            raise GRPC.RPCError,
              status: :internal,
              message: "bind unmount failed: #{inspect(reason)}"
        end
    end
  end

  @doc """
  CSI `Node.NodeGetVolumeStats` — reports per-mount usage and a
  `VolumeCondition`. The condition is derived from a host-local
  staging-path probe via `NeonFS.CSI.VolumeHealth.node_condition/3`;
  usage is read from the controller-side volume stats.

  The CSI spec lets either `volume_path` or `staging_target_path` be
  the probe target — kubelet usually sends the publish target. We
  prefer it; otherwise we fall back to the staged mount path on this
  node so unit/integration tests that exercise just the stage step
  still get a meaningful reply.
  """
  @spec node_get_volume_stats(NodeGetVolumeStatsRequest.t(), term()) ::
          NodeGetVolumeStatsResponse.t()
  def node_get_volume_stats(%NodeGetVolumeStatsRequest{volume_id: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_id is required"
  end

  def node_get_volume_stats(%NodeGetVolumeStatsRequest{volume_path: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_path is required"
  end

  def node_get_volume_stats(
        %NodeGetVolumeStatsRequest{
          volume_id: vol_id,
          volume_path: volume_path,
          staging_target_path: staging_path
        },
        _stream
      ) do
    init_state_tables()

    probe_path = preferred_probe_path(vol_id, volume_path, staging_path)
    condition = VolumeHealth.node_condition(vol_id, probe_path)
    usage = volume_usage(vol_id)

    %NodeGetVolumeStatsResponse{
      usage: usage,
      volume_condition: %VolumeCondition{
        abnormal: condition.abnormal,
        message: condition.message
      }
    }
  end

  ## Helpers

  defp preferred_probe_path(_vol_id, volume_path, _staging) when volume_path != "",
    do: volume_path

  defp preferred_probe_path(vol_id, _volume_path, staging) do
    case :ets.lookup(@staged_table, vol_id) do
      [{^vol_id, %{staging_path: path}}] -> path
      _ -> staging
    end
  end

  defp volume_usage(vol_id) do
    case core_call(NeonFS.Core, :get_volume, [vol_id]) do
      {:ok, volume} -> usage_for(volume_access_type(volume), volume)
      _ -> []
    end
  end

  # A capped volume reports its quota as `total`, so kubelet shows real
  # remaining space (`available = total - used`). A thin volume has no
  # ceiling, so `total = used` → `available = 0` rather than a misleading
  # negative from the `total - used` arithmetic.
  # A raw device has no filesystem, so it has no inodes and no free space
  # of its own: the whole device is in use by whatever the guest put there.
  # Reporting an inode count for it would be inventing a number, which is
  # worse than the spec's answer of total bytes alone.
  defp usage_for(:block, volume) do
    size = Map.get(volume, :max_size) || Map.get(volume, :logical_size, 0) || 0
    [%VolumeUsage{available: 0, total: size, used: size, unit: :BYTES}]
  end

  defp usage_for(_mount, volume) do
    [
      usage_entry(:BYTES, Map.get(volume, :logical_size, 0) || 0, Map.get(volume, :max_size)),
      usage_entry(:INODES, Map.get(volume, :file_count, 0) || 0, Map.get(volume, :max_files))
    ]
  end

  defp usage_entry(unit, used, nil) do
    %VolumeUsage{available: 0, total: used, used: used, unit: unit}
  end

  defp usage_entry(unit, used, max) when is_integer(max) do
    %VolumeUsage{available: max(max - used, 0), total: max, used: used, unit: unit}
  end

  defp core_call(module, function, args) do
    case Application.get_env(:neonfs_csi, :core_call_fn) do
      nil -> NeonFS.Client.Router.call(module, function, args)
      fun when is_function(fun, 3) -> fun.(module, function, args)
    end
  end

  defp do_stage(vol_id, staging_path, :mount) do
    with :ok <- File.mkdir_p(staging_path),
         {:ok, mount_id} <- fuse_mount_fn().(vol_id, staging_path) do
      :ets.insert(
        @staged_table,
        {vol_id, %{staging_path: staging_path, mount_id: mount_id, access_type: :mount}}
      )

      %NodeStageVolumeResponse{}
    else
      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "fuse mount failed: #{inspect(reason)}"
    end
  end

  # Staging a block volume attaches its device on this host. The staging
  # path is still created and still a directory — the spec says so
  # regardless of access type — but nothing is mounted on it; what is
  # staged is the device, and its path is what the publish needs.
  defp do_stage(vol_id, staging_path, :block) do
    with :ok <- File.mkdir_p(staging_path),
         {:ok, device_path} <- block_attach_fn().(vol_id) do
      :ets.insert(
        @staged_table,
        {vol_id, %{staging_path: staging_path, device_path: device_path, access_type: :block}}
      )

      %NodeStageVolumeResponse{}
    else
      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "block attach failed: #{inspect(reason)}"
    end
  end

  defp access_type(%{access_type: {:block, _}}), do: :block
  defp access_type(_capability), do: :mount

  # A filesystem operation against a block volume, or a block operation
  # against a filesystem one, is a mismatch the spec asks every RPC to
  # refuse rather than attempt. Staging is where the volume's own type is
  # first consulted; everything after it can compare against what was
  # staged.
  defp ensure_capability_matches_volume!(vol_id, cap) do
    case core_call(NeonFS.Core, :get_volume, [vol_id]) do
      {:ok, volume} ->
        refuse_mismatch!(access_type(cap), volume_access_type(volume), vol_id)

      _unresolved ->
        :ok
    end
  end

  defp ensure_capability_matches_staged!(vol_id, cap) do
    case staged_record(vol_id) do
      %{access_type: staged} -> refuse_mismatch!(access_type(cap), staged, vol_id)
      nil -> :ok
    end
  end

  defp refuse_mismatch!(same, same, _vol_id), do: :ok

  defp refuse_mismatch!(requested, actual, vol_id) do
    raise GRPC.RPCError,
      status: :invalid_argument,
      message:
        "volume #{vol_id} is a #{actual} volume; a #{requested} capability cannot be used with it"
  end

  defp volume_access_type(%{type: :block}), do: :block
  defp volume_access_type(_volume), do: :mount

  defp do_unstage(vol_id, %{access_type: :block, device_path: device_path}) do
    # A device that is already gone is the state this call is asking for.
    # Failing on it wedges the volume: the kubelet retries an unstage it
    # can never satisfy.
    case block_detach_fn().(device_path) do
      :ok ->
        :ets.delete(@staged_table, vol_id)
        %NodeUnstageVolumeResponse{}

      {:error, :not_attached} ->
        :ets.delete(@staged_table, vol_id)
        %NodeUnstageVolumeResponse{}

      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "block detach failed: #{inspect(reason)}"
    end
  end

  defp do_unstage(vol_id, %{mount_id: mount_id}) do
    case fuse_unmount_fn().(mount_id) do
      :ok ->
        :ets.delete(@staged_table, vol_id)
        %NodeUnstageVolumeResponse{}

      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "fuse unmount failed: #{inspect(reason)}"
    end
  end

  defp do_publish(vol_id, staging_path, target_path, readonly) do
    case staged_record(vol_id) do
      %{access_type: :block, device_path: device_path} ->
        publish_device(vol_id, staging_path, device_path, target_path, readonly)

      _mount ->
        publish_mount(vol_id, staging_path, target_path, readonly)
    end
  end

  defp publish_mount(vol_id, staging_path, target_path, readonly) do
    with :ok <- File.mkdir_p(target_path),
         :ok <- bind_mount_fn().(staging_path, target_path, readonly) do
      record_publish(vol_id, staging_path, target_path, readonly)
    else
      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "bind mount failed: #{inspect(reason)}"
    end
  end

  # For a block volume the kubelet's target path is a *file*, not a
  # directory: the device node is bind-mounted onto it. Creating a
  # directory there — which is what the mount path does — makes the bind
  # fail with a type mismatch, so the file has to exist first and be
  # exactly a file.
  defp publish_device(vol_id, staging_path, device_path, target_path, readonly) do
    with :ok <- File.mkdir_p(Path.dirname(target_path)),
         :ok <- touch_target_file(target_path),
         :ok <- bind_mount_fn().(device_path, target_path, readonly) do
      record_publish(vol_id, staging_path, target_path, readonly)
    else
      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "block bind mount failed: #{inspect(reason)}"
    end
  end

  defp touch_target_file(target_path) do
    cond do
      File.dir?(target_path) ->
        {:error, "target #{target_path} is a directory; a block target must be a file"}

      File.exists?(target_path) ->
        :ok

      true ->
        File.touch(target_path)
    end
  end

  defp record_publish(vol_id, staging_path, target_path, readonly) do
    :ets.insert(
      @published_table,
      {{vol_id, target_path}, %{staging_path: staging_path, readonly: readonly}}
    )

    %NodePublishVolumeResponse{}
  end

  defp staged_record(vol_id) do
    case :ets.lookup(@staged_table, vol_id) do
      [{^vol_id, record}] -> record
      [] -> nil
    end
  end

  defp ensure_staged!(vol_id, staging_path) do
    case :ets.lookup(@staged_table, vol_id) do
      [{^vol_id, %{staging_path: ^staging_path}}] ->
        :ok

      [{^vol_id, %{staging_path: existing}}] ->
        raise GRPC.RPCError,
          status: :failed_precondition,
          message: "volume #{vol_id} is staged at #{existing}, not #{staging_path}"

      [] ->
        raise GRPC.RPCError,
          status: :failed_precondition,
          message: "volume #{vol_id} is not staged"
    end
  end

  defp has_publishes?(vol_id) do
    :ets.match(@published_table, {{vol_id, :_}, :_}) != []
  end

  defp capability(rpc_type) do
    %NodeServiceCapability{
      type: {:rpc, %NodeServiceCapability.RPC{type: rpc_type}}
    }
  end

  @supported_modes [
    :SINGLE_NODE_WRITER,
    :MULTI_NODE_READER_ONLY,
    :MULTI_NODE_SINGLE_WRITER
  ]

  defp ensure_capability_supported!(%{access_mode: %{mode: mode}})
       when mode in @supported_modes,
       do: :ok

  defp ensure_capability_supported!(_) do
    raise GRPC.RPCError,
      status: :invalid_argument,
      message: "unsupported volume capability"
  end

  @doc """
  The identifier this plugin reports to the CO, and the one a controller
  has to resolve back to a BEAM node. Public because the service
  registration advertises it.
  """
  @spec node_id() :: String.t()
  def node_id do
    case System.get_env("NODE_ID") do
      val when is_binary(val) and val != "" -> val
      _ -> Application.get_env(:neonfs_csi, :node_id, to_string(Node.self()))
    end
  end

  ## Injection-fn defaults

  defp fuse_mount_fn do
    Application.get_env(:neonfs_csi, :fuse_mount_fn, &default_fuse_mount/2)
  end

  defp fuse_unmount_fn do
    Application.get_env(:neonfs_csi, :fuse_unmount_fn, &default_fuse_unmount/1)
  end

  defp bind_mount_fn do
    Application.get_env(:neonfs_csi, :bind_mount_fn, &default_bind_mount/3)
  end

  defp bind_unmount_fn do
    Application.get_env(:neonfs_csi, :bind_unmount_fn, &default_bind_unmount/1)
  end

  defp block_attach_fn do
    Application.get_env(:neonfs_csi, :block_attach_fn, &default_block_attach/1)
  end

  defp block_detach_fn do
    Application.get_env(:neonfs_csi, :block_detach_fn, &default_block_detach/1)
  end

  defp default_fuse_mount(volume_name, staging_path) do
    fuse_node = Application.get_env(:neonfs_csi, :fuse_node, Node.self())

    GenServer.call(
      {NeonFS.FUSE.MountManager, fuse_node},
      {:mount, volume_name, staging_path, []}
    )
  end

  defp default_fuse_unmount(mount_id) do
    fuse_node = Application.get_env(:neonfs_csi, :fuse_node, Node.self())
    GenServer.call({NeonFS.FUSE.MountManager, fuse_node}, {:unmount, mount_id})
  end

  defp default_bind_mount(staging_path, target_path, readonly?) do
    case System.cmd("mount", ["--bind", staging_path, target_path], stderr_to_stdout: true) do
      {_out, 0} -> maybe_remount_ro(target_path, readonly?)
      {out, code} -> {:error, "mount --bind exit #{code}: #{String.trim(out)}"}
    end
  end

  defp maybe_remount_ro(_target, false), do: :ok

  defp maybe_remount_ro(target_path, true) do
    case System.cmd("mount", ["-o", "remount,bind,ro", target_path], stderr_to_stdout: true) do
      {_out, 0} -> :ok
      {out, code} -> {:error, "remount,ro exit #{code}: #{String.trim(out)}"}
    end
  end

  defp default_bind_unmount(target_path) do
    case System.cmd("umount", [target_path], stderr_to_stdout: true) do
      {_out, 0} -> :ok
      {out, code} -> {:error, "umount exit #{code}: #{String.trim(out)}"}
    end
  end

  # A bare export name resolves to the volume's own device, which is the
  # form the block target answers and the one that does not require this
  # node to know the backing file's path.
  #
  # `-b 4096` is not decoration: nbd-client defaults to 512-byte blocks
  # whatever the server advertises, and the backing store refuses a
  # request that is not 4 KiB-aligned.
  defp default_block_attach(volume_id) do
    with {:ok, {host, port}} <- block_endpoint(),
         {:ok, device} <- free_nbd_device() do
      args = ["-N", volume_id, host, to_string(port), device, "-b", "4096", "-persist"]

      case System.cmd("nbd-client", args, stderr_to_stdout: true) do
        {_out, 0} -> {:ok, device}
        {out, code} -> {:error, "nbd-client exit #{code}: #{String.trim(out)}"}
      end
    end
  end

  defp default_block_detach(device_path) do
    case System.cmd("nbd-client", ["-d", device_path], stderr_to_stdout: true) do
      {_out, 0} -> :ok
      {out, code} -> {:error, "nbd-client -d exit #{code}: #{String.trim(out)}"}
    end
  end

  # The block target advertises where its NBD listener is as part of its
  # service registration, so a deployment does not have to be told twice.
  # An explicit config still wins, for a topology discovery cannot describe.
  defp block_endpoint do
    case Application.get_env(:neonfs_csi, :block_endpoint) do
      {host, port} when is_binary(host) and is_integer(port) -> {:ok, {host, port}}
      _unset -> discovered_block_endpoint()
    end
  end

  defp discovered_block_endpoint do
    :block
    |> Discovery.list_by_type()
    |> Enum.find_value(fn service ->
      case Map.get(service.metadata || %{}, :nbd_endpoint) do
        {host, port} when is_binary(host) and is_integer(port) -> {:ok, {host, port}}
        _absent -> nil
      end
    end)
    |> case do
      {:ok, _endpoint} = ok -> ok
      nil -> {:error, "no block service advertising an NBD endpoint was discovered"}
    end
  end

  # `nbd-client -c` reports whether a device is already bound, so the first
  # unbound one is free. Racing another attach on the same node is possible
  # and is what the non-zero exit from `nbd-client` then reports.
  defp free_nbd_device do
    Enum.find_value(0..15, {:error, "no free /dev/nbdX device"}, fn index ->
      device = "/dev/nbd#{index}"

      if File.exists?(device) and not nbd_device_bound?(device) do
        {:ok, device}
      end
    end)
  end

  defp nbd_device_bound?(device) do
    match?({_out, 0}, System.cmd("nbd-client", ["-c", device], stderr_to_stdout: true))
  end
end
