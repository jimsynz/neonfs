defmodule NeonFS.CSI.NodeServer do
  @moduledoc """
  CSI v1 Node service implementation. Runs as a Kubernetes DaemonSet
  pod (one per worker node) and performs the actual mount lifecycle
  on the host so pods can read/write NeonFS volumes.

  Sub-issue #315 of the CSI driver epic (#244). The Identity service
  (#313) advertises this plugin to the kubelet via the per-node socket;
  the Controller service (#314) handles cluster-wide volume lifecycle.

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
    NodePublishVolumeRequest,
    NodePublishVolumeResponse,
    NodeServiceCapability,
    NodeStageVolumeRequest,
    NodeStageVolumeResponse,
    NodeUnpublishVolumeRequest,
    NodeUnpublishVolumeResponse,
    NodeUnstageVolumeRequest,
    NodeUnstageVolumeResponse
  }

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
      capabilities: [capability(:STAGE_UNSTAGE_VOLUME)]
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

    case :ets.lookup(@staged_table, vol_id) do
      [{^vol_id, %{staging_path: ^staging_path}}] ->
        %NodeStageVolumeResponse{}

      [{^vol_id, %{staging_path: existing}}] ->
        raise GRPC.RPCError,
          status: :failed_precondition,
          message:
            "volume #{vol_id} is already staged at #{existing} (cannot re-stage at #{staging_path})"

      [] ->
        do_stage(vol_id, staging_path)
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

      [{^vol_id, %{staging_path: ^staging_path, mount_id: mount_id}}] ->
        if has_publishes?(vol_id) do
          raise GRPC.RPCError,
            status: :failed_precondition,
            message: "volume #{vol_id} still has active publishes; unpublish first"
        end

        case fuse_unmount_fn().(mount_id) do
          :ok ->
            :ets.delete(@staged_table, vol_id)
            %NodeUnstageVolumeResponse{}

          {:error, reason} ->
            raise GRPC.RPCError,
              status: :internal,
              message: "fuse unmount failed: #{inspect(reason)}"
        end

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

  ## Helpers

  defp do_stage(vol_id, staging_path) do
    with :ok <- File.mkdir_p(staging_path),
         {:ok, mount_id} <- fuse_mount_fn().(vol_id, staging_path) do
      :ets.insert(@staged_table, {vol_id, %{staging_path: staging_path, mount_id: mount_id}})
      %NodeStageVolumeResponse{}
    else
      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "fuse mount failed: #{inspect(reason)}"
    end
  end

  defp do_publish(vol_id, staging_path, target_path, readonly) do
    with :ok <- File.mkdir_p(target_path),
         :ok <- bind_mount_fn().(staging_path, target_path, readonly) do
      :ets.insert(
        @published_table,
        {{vol_id, target_path}, %{staging_path: staging_path, readonly: readonly}}
      )

      %NodePublishVolumeResponse{}
    else
      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "bind mount failed: #{inspect(reason)}"
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

  defp ensure_capability_supported!(%{access_type: {:block, _}}) do
    raise GRPC.RPCError,
      status: :invalid_argument,
      message: "block volumes are not supported"
  end

  defp ensure_capability_supported!(%{access_mode: %{mode: mode}})
       when mode in @supported_modes,
       do: :ok

  defp ensure_capability_supported!(_) do
    raise GRPC.RPCError,
      status: :invalid_argument,
      message: "unsupported volume capability"
  end

  defp node_id do
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
end
