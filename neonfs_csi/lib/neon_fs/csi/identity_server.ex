defmodule NeonFS.CSI.IdentityServer do
  @moduledoc """
  CSI v1 Identity service implementation. Required by every CSI
  plugin regardless of mode (Controller / Node).

  Implements three RPCs:

    * `GetPluginInfo` — returns the plugin's name, version, and a
      manifest map. The plugin name is the standard
      `neonfs.csi.harton.dev` reverse-DNS string Kubernetes uses to
      identify the driver in its registry.
    * `GetPluginCapabilities` — declares which optional CSI
      capabilities the plugin supports. The first slice declares
      `CONTROLLER_SERVICE` (the next sub-issue, #314, lands the real
      handlers behind it). `VOLUME_ACCESSIBILITY_CONSTRAINTS` is
      not yet supported.
    * `Probe` — health check. Returns `ready: true` when the cluster
      service registry can reach a core node, `ready: false`
      otherwise. The kubelet polls this every few seconds — the body
      must be fast.

  ## Mode awareness

  In `:controller` mode the plugin advertises Controller-side
  capabilities; in `:node` mode it doesn't. The mode is read from
  `Application.get_env(:neonfs_csi, :mode, :controller)`.
  """

  use GRPC.Server, service: Csi.V1.Identity.Service

  alias Csi.V1.{
    GetPluginCapabilitiesRequest,
    GetPluginCapabilitiesResponse,
    GetPluginInfoRequest,
    GetPluginInfoResponse,
    PluginCapability,
    ProbeRequest,
    ProbeResponse
  }

  alias Google.Protobuf.BoolValue
  alias NeonFS.Client.Discovery

  @driver_name "neonfs.csi.harton.dev"

  @doc """
  CSI `Identity.GetPluginInfo` RPC. Reports the canonical plugin
  name, the application version, and the configured mode in the
  manifest map so an operator can `kubectl describe csidriver`
  and see whether the pod they're looking at is the controller or
  the node-side plugin.
  """
  @spec get_plugin_info(GetPluginInfoRequest.t(), term()) :: GetPluginInfoResponse.t()
  def get_plugin_info(%GetPluginInfoRequest{}, _stream) do
    %GetPluginInfoResponse{
      name: @driver_name,
      vendor_version: to_string(Application.spec(:neonfs_csi, :vsn) || "0.0.0"),
      manifest: %{
        "mode" => to_string(Application.get_env(:neonfs_csi, :mode, :controller))
      }
    }
  end

  @doc """
  CSI `Identity.GetPluginCapabilities` RPC. Advertises the optional
  service-level capabilities Kubernetes uses to wire the plugin to
  the right sidecars (e.g. `CONTROLLER_SERVICE` makes the
  external-provisioner route CreateVolume / DeleteVolume here).
  """
  @spec get_plugin_capabilities(GetPluginCapabilitiesRequest.t(), term()) ::
          GetPluginCapabilitiesResponse.t()
  def get_plugin_capabilities(%GetPluginCapabilitiesRequest{}, _stream) do
    %GetPluginCapabilitiesResponse{
      capabilities: capabilities_for(Application.get_env(:neonfs_csi, :mode, :controller))
    }
  end

  @doc """
  CSI `Identity.Probe` RPC. Hot-path liveness check the kubelet
  polls every few seconds. Returns `ready: true` only when the
  local Discovery cache sees at least one core node.
  """
  @spec probe(ProbeRequest.t(), term()) :: ProbeResponse.t()
  def probe(%ProbeRequest{}, _stream) do
    %ProbeResponse{ready: %BoolValue{value: ready?()}}
  end

  ## Helpers

  # Controller mode advertises the CONTROLLER_SERVICE plugin
  # capability so the external-provisioner sidecar will route
  # CreateVolume / DeleteVolume to us. Node mode advertises no
  # plugin-level capabilities (Node service is implicit when the
  # kubelet talks to us via the Node socket). Both will gain
  # `VOLUME_ACCESSIBILITY_CONSTRAINTS` once #316 lands.
  defp capabilities_for(:controller) do
    [
      %PluginCapability{
        type:
          {:service,
           %PluginCapability.Service{
             type: :CONTROLLER_SERVICE
           }}
      }
    ]
  end

  defp capabilities_for(_), do: []

  # Probe is a tight-loop kubelet poll, so do the cheapest possible
  # liveness check: does the local Discovery cache see at least one
  # core node? When it doesn't, mark unready so the kubelet stops
  # routing to us until quorum is back.
  defp ready? do
    case Discovery.get_core_nodes() do
      [_ | _] -> true
      _ -> false
    end
  rescue
    _ -> false
  end
end
