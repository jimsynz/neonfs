defmodule NeonFS.MixProject do
  use Mix.Project
  @moduledoc false

  @version "0.1.0"

  def project,
    do: [
      app: :neonfs,
      deps: [
        {:ex_check, "~> 0.16", only: :dev, runtime: false},
        {:mix_audit, "~> 2.0", only: :dev, runtime: false}
      ],
      version: @version
    ]
end
