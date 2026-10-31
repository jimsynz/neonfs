defmodule NeonFS.Release.MixProject do
  use Mix.Project
  @moduledoc false

  # Hosts git_ops for this repo's releases: from here,
  # `MIX_ENV=dev mix git_ops.release`, then `git push --follow-tags`. The
  # NeonFS packages are versioned and released in their own repos.
  @version "0.6.1"

  def project,
    do: [
      app: :neonfs,
      deps: [
        {:git_ops, "~> 2.10", only: [:dev, :test], runtime: false}
      ],
      version: @version
    ]
end
