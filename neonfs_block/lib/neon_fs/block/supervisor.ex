defmodule NeonFS.Block.Supervisor do
  @moduledoc """
  Top-level supervisor for `neonfs_block`.

  Owns the device registry, the NBD listener, and the registrar that
  advertises this node as a `:block` service.

  The registry starts whether or not the listener does, and before it: a
  device outlives any one connection, so the thing tracking attachments must
  already exist when the first connection arrives.
  """

  use Supervisor

  alias NeonFS.Block.{DeviceRegistry, Listener}
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

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp maybe_add_registrar(children, false), do: children

  defp maybe_add_registrar(children, true) do
    children ++
      [
        {Registrar,
         metadata: registration_metadata(), type: :block, name: NeonFS.Client.Registrar.Block}
      ]
  end

  defp registration_metadata do
    %{
      capabilities: [:nbd],
      version: to_string(Application.spec(:neonfs_block, :vsn) || "0.0.0")
    }
  end
end
