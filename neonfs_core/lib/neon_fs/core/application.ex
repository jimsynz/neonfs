defmodule NeonFS.Core.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    # Get blob store configuration from application environment
    base_dir = Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs/blobs")
    prefix_depth = Application.get_env(:neonfs_core, :blob_store_prefix_depth, 2)

    children = [
      NeonFS.Core.VolumeRegistry,
      NeonFS.Core.ChunkIndex,
      NeonFS.Core.FileIndex,
      {NeonFS.Core.BlobStore, base_dir: base_dir, prefix_depth: prefix_depth}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: NeonFS.Core.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
