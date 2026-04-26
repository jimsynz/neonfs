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
    CreateVolumeRequest,
    CreateVolumeResponse,
    DeleteVolumeRequest,
    DeleteVolumeResponse,
    GetCapacityRequest,
    GetCapacityResponse,
    ListVolumesRequest,
    ListVolumesResponse,
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
            :VOLUME_CONDITION
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
