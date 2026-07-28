defmodule NeonFS.Release.MixProject do
  use Mix.Project
  @moduledoc false

  # Hosts git_ops for cutting workspace releases — run via
  # `resources/scripts/neonfs-release`. Every package's version is kept in
  # lockstep with this one by the managed_files glob in config/config.exs.
  @version "0.5.1"

  def project,
    do: [
      app: :neonfs,
      deps: [
        {:git_ops, "~> 2.10", only: [:dev, :test], runtime: false}
      ],
      version: @version
    ]
end
