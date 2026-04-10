defmodule NeonFS.S3.Supervisor do
  @moduledoc """
  Top-level supervisor for neonfs_s3 application.

  Supervises:
  - Client connectivity (Connection, Discovery, CostFunction)
  - Service registration with the core cluster
  - MultipartStore for tracking in-progress multipart uploads
  - Bandit HTTP server running S3Server.Plug
  """

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    port = Application.get_env(:neonfs_s3, :s3_port, 8080)
    bind = Application.get_env(:neonfs_s3, :s3_bind, "0.0.0.0")

    children = [
      {NeonFS.Client.Registrar,
       metadata: registration_metadata(), type: :s3, name: NeonFS.Client.Registrar.S3},
      NeonFS.S3.MultipartStore,
      {Bandit,
       plug: {S3Server.Plug, backend: NeonFS.S3.Backend},
       port: port,
       ip: parse_bind_address(bind),
       scheme: :http}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp registration_metadata do
    %{
      capabilities: [:read, :write],
      version: to_string(Application.spec(:neonfs_s3, :vsn) || "0.0.0")
    }
  end

  defp parse_bind_address(bind) when is_binary(bind) do
    {:ok, ip} = :inet.parse_address(String.to_charlist(bind))
    ip
  end
end
