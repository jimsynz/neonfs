defmodule NeonFS.Block.Supervisor do
  @moduledoc """
  Top-level supervisor for `neonfs_block`.

  Owns the device registry, the NBD listener, and the registrar that
  advertises this node as a `:block` service.

  The registry starts whether or not the listener does, and before it: a
  device outlives any one connection, so the thing tracking attachments must
  already exist when the first connection arrives.

  ## Why `:rest_for_one`

  That ordering is a dependency, not a preference, so a registry that dies
  has to take the listener with it. The registry holds each attached device's
  exclusive attachment claim, and the coordinator releases every claim a dead
  holder owned — while the connections it served are their own processes and
  carry on reading and writing. `:one_for_one` would leave exactly that: live
  NBD traffic against a device the cluster believes is unattached, which the
  next attach through either route would then be admitted to.

  Restarting the listener closes those connections, which is the honest
  outcome — their claims are already gone. The reverse direction needs
  nothing: a listener that dies takes its connections with it, and the
  registry's monitors release their claims.
  """

  use Supervisor

  alias NeonFS.Block.{DeviceRegistry, Listener, MetricsSupervisor}
  alias NeonFS.Client.Registrar

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(opts) do
    register? = Application.get_env(:neonfs_block, :register_service, true)

    children =
      [DeviceRegistry, Listener.child_spec(opts)]
      |> maybe_add_registrar(register?)
      |> Kernel.++(metrics_children())

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp maybe_add_registrar(children, false), do: children

  defp maybe_add_registrar(children, true) do
    children ++
      [
        {Registrar,
         metadata: registration_metadata(), type: :block, name: NeonFS.Client.Registrar.Block}
      ]
  end

  defp metrics_children do
    if MetricsSupervisor.enabled?(), do: [MetricsSupervisor], else: []
  end

  # The endpoint travels with the registration because a service that
  # cannot be dialled is not much of a discovery: CSI's node plugin needs a
  # host and port to hand `nbd-client`, and this is the only place that
  # knows what the listener bound.
  defp registration_metadata do
    %{
      capabilities: [:nbd],
      nbd_endpoint: {advertised_host(), Listener.port()},
      version: to_string(Application.spec(:neonfs_block, :vsn) || "0.0.0")
    }
  end

  # A loopback bind is only reachable from the node itself, and telling a
  # peer to dial 127.0.0.1 would send it to its own machine. The configured
  # advertise address wins; otherwise a non-loopback bind is its own answer.
  defp advertised_host do
    case Application.get_env(:neonfs_block, :advertise) do
      host when is_binary(host) and host != "" ->
        host

      _unset ->
        Listener.bind_address() |> :inet.ntoa() |> to_string()
    end
  end
end
