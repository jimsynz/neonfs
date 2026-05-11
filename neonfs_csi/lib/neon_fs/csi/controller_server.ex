defmodule NeonFS.CSI.ControllerServer do
  @moduledoc """
  CSI v1 Controller service implementation. Runs as the cluster
  control-plane bridge between Kubernetes PersistentVolumeClaims and
  NeonFS volumes.

  Sub-issue #314 of the CSI driver epic (#244). The Identity service
  (#313) declares this plugin's `CONTROLLER_SERVICE` capability so the
  external-provisioner sidecar routes CreateVolume / DeleteVolume
  here.

  ## RPCs implemented

    * `ControllerGetCapabilities` — advertises
      `CREATE_DELETE_VOLUME`, `PUBLISH_UNPUBLISH_VOLUME`,
      `LIST_VOLUMES`, `GET_CAPACITY`.
    * `CreateVolume` — maps to `NeonFS.Core.create_volume/2`.
      Honours `parameters` (`replication_factor`, `tier`) and the
      requested `capacity_range`.
    * `DeleteVolume` — maps to `NeonFS.Core.delete_volume/1` with
      idempotent `:ok` on `:not_found`.
    * `ValidateVolumeCapabilities` — confirms requested access
      modes against the volume's supported set.
    * `ListVolumes` — paginated list via `NeonFS.Core.list_volumes/0`
      with CSI string-token pagination.
    * `GetCapacity` — cluster-wide via
      `NeonFS.Core.StorageMetrics.cluster_capacity/0`.
    * `ControllerPublishVolume` / `ControllerUnpublishVolume` — track
      `(volume_id, node_id)` publish state in a local ETS table that
      the Node plugin consults during Stage.
    * `ControllerGetVolume` — returns the per-volume `Volume` plus a
      `VolumeStatus` containing the current published-node ids and
      a `VolumeCondition` rolled up from `NeonFS.Core.StorageMetrics`,
      `NeonFS.Core.ServiceRegistry`, and `NeonFS.Core.Escalation` via
      `NeonFS.CSI.VolumeHealth`.

  Out of scope for this slice (snapshot RPCs, `ControllerExpandVolume`,
  `ControllerModifyVolume`) — those gain capability-flag entries here
  when their respective sub-issues land.

  ## Test injection

  All NeonFS RPCs route through `core_call/3`, which falls back to
  `NeonFS.Client.Router.call/3` unless `:core_call_fn` is set in the
  application env. The publish table lives in ETS (`@table`); tests
  reset it via `reset_publish_table/0`.
  """

  use GRPC.Server, service: Csi.V1.Controller.Service

  alias Csi.V1.{
    ControllerGetCapabilitiesRequest,
    ControllerGetCapabilitiesResponse,
    ControllerGetVolumeRequest,
    ControllerGetVolumeResponse,
    ControllerPublishVolumeRequest,
    ControllerPublishVolumeResponse,
    ControllerServiceCapability,
    ControllerUnpublishVolumeRequest,
    ControllerUnpublishVolumeResponse,
    CreateSnapshotRequest,
    CreateSnapshotResponse,
    CreateVolumeRequest,
    CreateVolumeResponse,
    DeleteSnapshotRequest,
    DeleteSnapshotResponse,
    DeleteVolumeRequest,
    DeleteVolumeResponse,
    GetCapacityRequest,
    GetCapacityResponse,
    ListSnapshotsRequest,
    ListSnapshotsResponse,
    ListVolumesRequest,
    ListVolumesResponse,
    Snapshot,
    ValidateVolumeCapabilitiesRequest,
    ValidateVolumeCapabilitiesResponse,
    Volume,
    VolumeCondition
  }

  alias NeonFS.Client.Router
  alias NeonFS.CSI.VolumeHealth

  import Bitwise, only: [<<<: 2]

  @table :csi_published_volumes

  @doc """
  Initialise the ETS table backing the publish state. Called once by
  the supervisor; idempotent.
  """
  @spec init_publish_table() :: :ok
  def init_publish_table do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    end

    :ok
  end

  @doc "Clears the publish table. Test-only convenience."
  @spec reset_publish_table() :: :ok
  def reset_publish_table do
    init_publish_table()
    :ets.delete_all_objects(@table)
    :ok
  end

  ## RPCs

  @doc "CSI `Controller.ControllerGetCapabilities` — declares the supported RPCs."
  @spec controller_get_capabilities(ControllerGetCapabilitiesRequest.t(), term()) ::
          ControllerGetCapabilitiesResponse.t()
  def controller_get_capabilities(%ControllerGetCapabilitiesRequest{}, _stream) do
    %ControllerGetCapabilitiesResponse{
      capabilities:
        Enum.map(
          [
            :CREATE_DELETE_VOLUME,
            :PUBLISH_UNPUBLISH_VOLUME,
            :LIST_VOLUMES,
            :GET_CAPACITY,
            :GET_VOLUME,
            :VOLUME_CONDITION,
            :CREATE_DELETE_SNAPSHOT,
            :LIST_SNAPSHOTS,
            :CLONE_VOLUME
          ],
          &capability/1
        )
    }
  end

  @doc """
  CSI `Controller.CreateVolume` — provisions a NeonFS volume from the
  Kubernetes PVC. Maps the CSI request name into the volume name and
  forwards user-supplied `parameters` (`replication_factor`, `tier`)
  to `NeonFS.Core.create_volume/2`.
  """
  @spec create_volume(CreateVolumeRequest.t(), term()) :: CreateVolumeResponse.t()
  def create_volume(
        %CreateVolumeRequest{name: name, volume_content_source: source} = req,
        _stream
      )
      when is_binary(name) and name != "" and not is_nil(source) do
    create_volume_from_source(req, source)
  end

  def create_volume(%CreateVolumeRequest{name: name} = req, _stream)
      when is_binary(name) and name != "" do
    opts = parse_create_opts(req)

    case core_call(NeonFS.Core, :create_volume, [name, opts]) do
      {:ok, volume} ->
        %CreateVolumeResponse{volume: csi_volume_from(volume, requested_capacity(req))}

      {:error, :exists} ->
        case core_call(NeonFS.Core, :get_volume, [name]) do
          {:ok, volume} ->
            %CreateVolumeResponse{volume: csi_volume_from(volume, requested_capacity(req))}

          _ ->
            raise GRPC.RPCError, status: :already_exists, message: "volume #{name} exists"
        end

      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "create_volume failed: #{inspect(reason)}"
    end
  end

  def create_volume(%CreateVolumeRequest{}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "name is required"
  end

  # CSI v1 CreateVolume with `VolumeContentSource` — the new volume's
  # content is sourced from either an existing snapshot or another
  # volume (clone). Both paths land on `Snapshot.promote/4`, which
  # creates a volume that shares the source's content-addressed
  # chunk graph — no bytes are copied.
  defp create_volume_from_source(req, %{type: {:snapshot, %{snapshot_id: csi_snap_id}}})
       when is_binary(csi_snap_id) and csi_snap_id != "" do
    case decode_csi_snapshot_id(csi_snap_id) do
      {:ok, vol_name, snap_id} ->
        promote_into_new_volume(req, vol_name, snap_id)

      :error ->
        raise GRPC.RPCError,
          status: :invalid_argument,
          message: "malformed snapshot id #{csi_snap_id}; expected \"<volume>/<snapshot>\""
    end
  end

  defp create_volume_from_source(req, %{type: {:volume, %{volume_id: source_volume_id}}})
       when is_binary(source_volume_id) and source_volume_id != "" do
    with {:ok, source_volume} <- lookup_volume(source_volume_id),
         {:ok, snap} <- create_clone_snapshot(source_volume.id, req.name) do
      promote_into_new_volume(req, source_volume.name, snap.id)
    else
      {:error, :not_found} ->
        raise GRPC.RPCError,
          status: :not_found,
          message: "source volume #{source_volume_id} not found"

      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "clone failed: #{inspect(reason)}"
    end
  end

  defp create_volume_from_source(_req, _bad) do
    raise GRPC.RPCError,
      status: :invalid_argument,
      message: "volume_content_source must specify either a snapshot or a volume"
  end

  defp create_clone_snapshot(source_volume_id, requested_name) do
    name = "csi-clone-source-#{requested_name}"

    core_call(NeonFS.Core.Snapshot, :create, [source_volume_id, [name: name]])
  end

  defp promote_into_new_volume(req, source_volume_name, snapshot_id) do
    with {:ok, source_volume} <- lookup_volume(source_volume_name),
         {:ok, new_volume} <-
           core_call(NeonFS.Core.Snapshot, :promote, [
             source_volume.id,
             snapshot_id,
             req.name,
             []
           ]) do
      %CreateVolumeResponse{
        volume:
          csi_volume_from(new_volume, requested_capacity(req))
          |> with_content_source(req.volume_content_source)
      }
    else
      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "source not found"

      {:error, :volume_not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "source volume not found"

      {:error, reason} ->
        raise GRPC.RPCError, status: :internal, message: "promote failed: #{inspect(reason)}"
    end
  end

  defp with_content_source(%Volume{} = vol, content_source) when not is_nil(content_source) do
    %Volume{vol | content_source: content_source}
  end

  defp with_content_source(vol, _), do: vol

  @doc """
  CSI `Controller.DeleteVolume` — idempotent: a `:not_found` from
  core resolves to a successful empty reply (Kubernetes retries
  delete on volumes that may already be gone).
  """
  @spec delete_volume(DeleteVolumeRequest.t(), term()) :: DeleteVolumeResponse.t()
  def delete_volume(%DeleteVolumeRequest{volume_id: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_id is required"
  end

  def delete_volume(%DeleteVolumeRequest{volume_id: volume_id}, _stream) do
    case core_call(NeonFS.Core, :delete_volume, [volume_id]) do
      :ok ->
        :ets.match_delete(@table, {{volume_id, :_}, :_})
        %DeleteVolumeResponse{}

      {:error, :not_found} ->
        %DeleteVolumeResponse{}

      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "delete_volume failed: #{inspect(reason)}"
    end
  end

  @doc """
  CSI `Controller.ValidateVolumeCapabilities` — confirms requested
  access modes against the volume's supported set. Returns a
  `confirmed` reply when every requested capability is supported,
  otherwise an empty reply with `message`.
  """
  @spec validate_volume_capabilities(ValidateVolumeCapabilitiesRequest.t(), term()) ::
          ValidateVolumeCapabilitiesResponse.t()
  def validate_volume_capabilities(
        %ValidateVolumeCapabilitiesRequest{volume_id: ""},
        _stream
      ) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_id is required"
  end

  def validate_volume_capabilities(
        %ValidateVolumeCapabilitiesRequest{
          volume_id: volume_id,
          volume_capabilities: caps
        },
        _stream
      ) do
    case core_call(NeonFS.Core, :get_volume, [volume_id]) do
      {:ok, _volume} ->
        if all_capabilities_supported?(caps) do
          %ValidateVolumeCapabilitiesResponse{
            confirmed: %ValidateVolumeCapabilitiesResponse.Confirmed{volume_capabilities: caps}
          }
        else
          %ValidateVolumeCapabilitiesResponse{
            message: "one or more requested capabilities are not supported"
          }
        end

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "volume #{volume_id} not found"

      {:error, reason} ->
        raise GRPC.RPCError, status: :internal, message: "lookup failed: #{inspect(reason)}"
    end
  end

  @doc """
  CSI `Controller.ListVolumes` — paginated. The `starting_token` is
  the volume name to resume from (CSI tokens are opaque strings); we
  use the volume name verbatim because it's already a stable cluster
  identifier.
  """
  @spec list_volumes(ListVolumesRequest.t(), term()) :: ListVolumesResponse.t()
  def list_volumes(%ListVolumesRequest{} = req, _stream) do
    {:ok, volumes} = core_call(NeonFS.Core, :list_volumes, [])

    sorted = Enum.sort_by(volumes, & &1.name)
    paginated = paginate(sorted, req.starting_token, max(req.max_entries, 0))

    next_token =
      case List.last(paginated.entries) do
        nil -> ""
        last -> if paginated.more?, do: last.name, else: ""
      end

    %ListVolumesResponse{
      entries:
        Enum.map(paginated.entries, fn vol ->
          %ListVolumesResponse.Entry{volume: csi_volume_from(vol, 0)}
        end),
      next_token: next_token
    }
  end

  @doc """
  CSI `Controller.GetCapacity` — reports cluster-wide available
  bytes. Capability filters (`accessible_topology`,
  `volume_capabilities`, `parameters`) are accepted but not used
  yet — accessibility constraints land with #316.
  """
  @spec get_capacity(GetCapacityRequest.t(), term()) :: GetCapacityResponse.t()
  def get_capacity(%GetCapacityRequest{}, _stream) do
    capacity =
      case core_call(NeonFS.Core.StorageMetrics, :cluster_capacity, []) do
        %{total_available: :unlimited} -> 1 <<< 62
        %{total_available: bytes} when is_integer(bytes) -> bytes
        _ -> 0
      end

    %GetCapacityResponse{available_capacity: capacity}
  end

  @doc """
  CSI `Controller.ControllerPublishVolume` — records a publish
  attachment for `(volume_id, node_id)` in the local ETS table.
  Idempotent on a re-publish for the same pair.
  """
  @spec controller_publish_volume(ControllerPublishVolumeRequest.t(), term()) ::
          ControllerPublishVolumeResponse.t()
  def controller_publish_volume(
        %ControllerPublishVolumeRequest{volume_id: ""},
        _stream
      ) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_id is required"
  end

  def controller_publish_volume(
        %ControllerPublishVolumeRequest{node_id: ""},
        _stream
      ) do
    raise GRPC.RPCError, status: :invalid_argument, message: "node_id is required"
  end

  def controller_publish_volume(
        %ControllerPublishVolumeRequest{volume_id: vol_id, node_id: node_id, readonly: ro},
        _stream
      ) do
    init_publish_table()

    case core_call(NeonFS.Core, :get_volume, [vol_id]) do
      {:ok, _volume} ->
        publish_context = %{"readonly" => to_string(ro)}
        :ets.insert(@table, {{vol_id, node_id}, publish_context})

        %ControllerPublishVolumeResponse{publish_context: publish_context}

      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "volume #{vol_id} not found"

      {:error, reason} ->
        raise GRPC.RPCError, status: :internal, message: "lookup failed: #{inspect(reason)}"
    end
  end

  @doc """
  CSI `Controller.ControllerUnpublishVolume` — clears the publish
  attachment. Idempotent: unknown `(volume_id, node_id)` pairs
  return success.
  """
  @spec controller_unpublish_volume(ControllerUnpublishVolumeRequest.t(), term()) ::
          ControllerUnpublishVolumeResponse.t()
  def controller_unpublish_volume(
        %ControllerUnpublishVolumeRequest{volume_id: ""},
        _stream
      ) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_id is required"
  end

  def controller_unpublish_volume(
        %ControllerUnpublishVolumeRequest{volume_id: vol_id, node_id: node_id},
        _stream
      ) do
    init_publish_table()

    case node_id do
      "" -> :ets.match_delete(@table, {{vol_id, :_}, :_})
      _ -> :ets.delete(@table, {vol_id, node_id})
    end

    %ControllerUnpublishVolumeResponse{}
  end

  @doc """
  CSI `Controller.ControllerGetVolume` — returns the volume plus a
  `VolumeStatus` carrying the current published-node ids and a
  cluster-wide `VolumeCondition` derived from `NeonFS.CSI.VolumeHealth`.

  Raises `NOT_FOUND` if the volume is gone.
  """
  @spec controller_get_volume(ControllerGetVolumeRequest.t(), term()) ::
          ControllerGetVolumeResponse.t()
  def controller_get_volume(%ControllerGetVolumeRequest{volume_id: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "volume_id is required"
  end

  def controller_get_volume(%ControllerGetVolumeRequest{volume_id: vol_id}, _stream) do
    init_publish_table()

    with {:ok, volume} <- core_call(NeonFS.Core, :get_volume, [vol_id]),
         {:ok, condition} <- VolumeHealth.controller_condition(vol_id) do
      %ControllerGetVolumeResponse{
        volume: csi_volume_from(volume, 0),
        status: %ControllerGetVolumeResponse.VolumeStatus{
          published_node_ids: published_node_ids(vol_id),
          volume_condition: %VolumeCondition{
            abnormal: condition.abnormal,
            message: condition.message
          }
        }
      }
    else
      {:error, :not_found} ->
        raise GRPC.RPCError, status: :not_found, message: "volume #{vol_id} not found"

      {:error, reason} ->
        raise GRPC.RPCError, status: :internal, message: "lookup failed: #{inspect(reason)}"
    end
  end

  @doc """
  CSI `Controller.CreateSnapshot` — creates a snapshot of the volume
  identified by `source_volume_id` (the volume's NeonFS name). The
  returned `snapshot_id` is `"<volume_name>/<snapshot_uuid>"` so
  `DeleteSnapshot` and `ListSnapshots(snapshot_id: …)` can resolve
  back to the volume without a second lookup.
  """
  @spec create_snapshot(CreateSnapshotRequest.t(), term()) :: CreateSnapshotResponse.t()
  def create_snapshot(%CreateSnapshotRequest{source_volume_id: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "source_volume_id is required"
  end

  def create_snapshot(%CreateSnapshotRequest{name: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "name is required"
  end

  def create_snapshot(
        %CreateSnapshotRequest{source_volume_id: source_volume_id, name: name},
        _stream
      ) do
    case lookup_volume(source_volume_id) do
      {:ok, volume} ->
        case core_call(NeonFS.Core.Snapshot, :create, [volume.id, [name: name]]) do
          {:ok, snap} ->
            %CreateSnapshotResponse{
              snapshot: csi_snapshot_from(snap, volume)
            }

          {:error, reason} ->
            raise GRPC.RPCError,
              status: :internal,
              message: "create_snapshot failed: #{inspect(reason)}"
        end

      {:error, :not_found} ->
        raise GRPC.RPCError,
          status: :not_found,
          message: "source volume #{source_volume_id} not found"

      {:error, reason} ->
        raise GRPC.RPCError, status: :internal, message: "lookup failed: #{inspect(reason)}"
    end
  end

  @doc """
  CSI `Controller.DeleteSnapshot` — idempotent. Parses the
  `"<volume_name>/<snapshot_uuid>"` snapshot id, resolves the
  volume, and calls `Snapshot.delete/2`. Returns success for
  unknown ids.
  """
  @spec delete_snapshot(DeleteSnapshotRequest.t(), term()) :: DeleteSnapshotResponse.t()
  def delete_snapshot(%DeleteSnapshotRequest{snapshot_id: ""}, _stream) do
    raise GRPC.RPCError, status: :invalid_argument, message: "snapshot_id is required"
  end

  def delete_snapshot(%DeleteSnapshotRequest{snapshot_id: csi_snap_id}, _stream) do
    # Malformed ids and missing volumes are both treated as idempotent
    # successes — CSI retries Delete on snapshots that may already be
    # gone or that the client has lost track of.
    case decode_csi_snapshot_id(csi_snap_id) do
      {:ok, vol_name, snap_id} -> delete_snapshot_for(vol_name, snap_id)
      :error -> %DeleteSnapshotResponse{}
    end
  end

  defp delete_snapshot_for(vol_name, snap_id) do
    case lookup_volume(vol_name) do
      {:ok, volume} -> do_delete_snapshot(volume.id, snap_id)
      {:error, :not_found} -> %DeleteSnapshotResponse{}
    end
  end

  defp do_delete_snapshot(volume_id, snap_id) do
    case core_call(NeonFS.Core.Snapshot, :delete, [volume_id, snap_id]) do
      :ok ->
        %DeleteSnapshotResponse{}

      {:error, reason} ->
        raise GRPC.RPCError,
          status: :internal,
          message: "delete_snapshot failed: #{inspect(reason)}"
    end
  end

  @doc """
  CSI `Controller.ListSnapshots` — supports filtering by
  `source_volume_id`, by `snapshot_id`, or unfiltered (list every
  snapshot in the cluster). Paginated via the standard
  `starting_token` + `max_entries` mechanism.
  """
  @spec list_snapshots(ListSnapshotsRequest.t(), term()) :: ListSnapshotsResponse.t()
  def list_snapshots(%ListSnapshotsRequest{snapshot_id: csi_snap_id}, _stream)
      when is_binary(csi_snap_id) and csi_snap_id != "" do
    list_snapshots_by_id(csi_snap_id)
  end

  def list_snapshots(%ListSnapshotsRequest{source_volume_id: vol_name} = req, _stream)
      when is_binary(vol_name) and vol_name != "" do
    case lookup_volume(vol_name) do
      {:ok, volume} ->
        case core_call(NeonFS.Core.Snapshot, :list, [volume.id]) do
          {:ok, snapshots} ->
            paginate_snapshots(snapshots, volume, req)

          {:error, reason} ->
            raise GRPC.RPCError,
              status: :internal,
              message: "list_snapshots failed: #{inspect(reason)}"
        end

      {:error, :not_found} ->
        # No volume → no snapshots.
        %ListSnapshotsResponse{entries: [], next_token: ""}
    end
  end

  def list_snapshots(%ListSnapshotsRequest{} = req, _stream) do
    # No filter — walk every volume's snapshots. Cheap because
    # snapshots are pointer entries, not chunk data.
    {:ok, volumes} = core_call(NeonFS.Core, :list_volumes, [])

    pairs =
      Enum.flat_map(volumes, fn volume ->
        case core_call(NeonFS.Core.Snapshot, :list, [volume.id]) do
          {:ok, snapshots} -> Enum.map(snapshots, &{&1, volume})
          _ -> []
        end
      end)

    paginate_snapshot_pairs(pairs, req)
  end

  defp list_snapshots_by_id(csi_snap_id) do
    case decode_csi_snapshot_id(csi_snap_id) do
      {:ok, vol_name, snap_id} ->
        with {:ok, volume} <- lookup_volume(vol_name),
             {:ok, snap} <- core_call(NeonFS.Core.Snapshot, :get, [volume.id, snap_id]) do
          %ListSnapshotsResponse{
            entries: [%ListSnapshotsResponse.Entry{snapshot: csi_snapshot_from(snap, volume)}],
            next_token: ""
          }
        else
          _ -> %ListSnapshotsResponse{entries: [], next_token: ""}
        end

      :error ->
        %ListSnapshotsResponse{entries: [], next_token: ""}
    end
  end

  defp paginate_snapshots(snapshots, volume, %ListSnapshotsRequest{} = req) do
    pairs = Enum.map(snapshots, &{&1, volume})
    paginate_snapshot_pairs(pairs, req)
  end

  defp paginate_snapshot_pairs(pairs, %ListSnapshotsRequest{
         starting_token: token,
         max_entries: max
       }) do
    sorted = Enum.sort_by(pairs, fn {snap, _vol} -> snap.id end)
    page = paginate_pairs(sorted, token, max(max, 0))

    entries =
      Enum.map(page.entries, fn {snap, vol} ->
        %ListSnapshotsResponse.Entry{snapshot: csi_snapshot_from(snap, vol)}
      end)

    next_token =
      case List.last(page.entries) do
        nil -> ""
        {snap, _vol} -> if page.more?, do: snap.id, else: ""
      end

    %ListSnapshotsResponse{entries: entries, next_token: next_token}
  end

  defp paginate_pairs(pairs, "", 0), do: %{entries: pairs, more?: false}
  defp paginate_pairs(pairs, "", max), do: take_pair_page(pairs, max)

  defp paginate_pairs(pairs, token, max) do
    rest = Enum.drop_while(pairs, fn {snap, _vol} -> snap.id <= token end)

    case max do
      0 -> %{entries: rest, more?: false}
      _ -> take_pair_page(rest, max)
    end
  end

  defp take_pair_page(pairs, max) do
    {entries, rest} = Enum.split(pairs, max)
    %{entries: entries, more?: rest != []}
  end

  defp csi_snapshot_from(snap, volume) do
    %Snapshot{
      snapshot_id: "#{volume.name}/#{snap.id}",
      source_volume_id: volume.name,
      creation_time: datetime_to_timestamp(snap.created_at),
      ready_to_use: true,
      size_bytes: 0
    }
  end

  defp datetime_to_timestamp(%DateTime{} = dt) do
    unix = DateTime.to_unix(dt, :nanosecond)
    %Google.Protobuf.Timestamp{seconds: div(unix, 1_000_000_000), nanos: rem(unix, 1_000_000_000)}
  end

  defp datetime_to_timestamp(_), do: nil

  defp decode_csi_snapshot_id(csi_snap_id) do
    case String.split(csi_snap_id, "/", parts: 2) do
      [vol_name, snap_id] when vol_name != "" and snap_id != "" -> {:ok, vol_name, snap_id}
      _ -> :error
    end
  end

  defp lookup_volume(name_or_id) do
    core_call(NeonFS.Core, :get_volume, [name_or_id])
  end

  ## Helpers

  defp capability(rpc_type) do
    %ControllerServiceCapability{
      type:
        {:rpc,
         %ControllerServiceCapability.RPC{
           type: rpc_type
         }}
    }
  end

  defp parse_create_opts(%CreateVolumeRequest{parameters: parameters}) do
    parameters
    |> Map.new(fn {k, v} -> {k, v} end)
    |> Enum.flat_map(&parse_create_opt/1)
  end

  defp parse_create_opt({"replication_factor", value}) do
    case Integer.parse(value) do
      {n, ""} when n > 0 -> [replication_factor: n]
      _ -> []
    end
  end

  defp parse_create_opt({"tier", value}) when value in ["hot", "warm", "cold"] do
    [tier: String.to_atom(value)]
  end

  defp parse_create_opt(_), do: []

  defp requested_capacity(%CreateVolumeRequest{capacity_range: nil}), do: 0
  defp requested_capacity(%CreateVolumeRequest{capacity_range: %{required_bytes: n}}), do: n

  defp csi_volume_from(volume, capacity) do
    %Volume{
      capacity_bytes: capacity,
      volume_id: volume.name,
      volume_context: %{"volume_id" => volume.id}
    }
  end

  defp published_node_ids(vol_id) do
    @table
    |> :ets.match({{vol_id, :"$1"}, :_})
    |> List.flatten()
  end

  # Supported access modes mirror the NeonFS data plane: a single
  # node can write while many nodes read. Block volumes aren't
  # supported (NeonFS exposes a filesystem, not a raw device).
  @supported_modes [
    :SINGLE_NODE_WRITER,
    :MULTI_NODE_READER_ONLY,
    :MULTI_NODE_SINGLE_WRITER
  ]

  defp all_capabilities_supported?(caps) when is_list(caps) and caps != [] do
    Enum.all?(caps, &capability_supported?/1)
  end

  defp all_capabilities_supported?(_), do: false

  defp capability_supported?(%{access_type: {:block, _}}), do: false

  defp capability_supported?(%{access_mode: %{mode: mode}}) when mode in @supported_modes,
    do: true

  defp capability_supported?(_), do: false

  defp paginate(volumes, "", 0), do: %{entries: volumes, more?: false}
  defp paginate(volumes, "", max), do: take_page(volumes, max)

  defp paginate(volumes, token, max) do
    rest = Enum.drop_while(volumes, &(&1.name <= token))

    case max do
      0 -> %{entries: rest, more?: false}
      _ -> take_page(rest, max)
    end
  end

  defp take_page(volumes, max) do
    {entries, rest} = Enum.split(volumes, max)
    %{entries: entries, more?: rest != []}
  end

  defp core_call(module, function, args) do
    case Application.get_env(:neonfs_csi, :core_call_fn) do
      nil -> Router.call(module, function, args)
      fun when is_function(fun, 3) -> fun.(module, function, args)
    end
  end
end
