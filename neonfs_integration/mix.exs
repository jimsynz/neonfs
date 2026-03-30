defmodule NeonFS.Integration.MixProject do
  use Mix.Project

  @moduledoc """
  Multi-node integration test suite for NeonFS.
  """
  @version "0.1.1"

  def project do
    [
      app: :neonfs_integration,
      deps: deps(),
      description: @moduledoc,
      dialyzer: dialyzer(),
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      version: @version
    ]
  end

  defp dialyzer do
    [
      ignore_warnings: ".dialyzer_ignore.exs",
      plt_add_apps: [:neonfs_client, :neonfs_core, :neonfs_fuse]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # Path dependencies for testing (runtime: false since only used in tests)
      {:neonfs_core, path: "../neonfs_core", runtime: false},
      {:neonfs_fuse, path: "../neonfs_fuse", runtime: false},

      # dev/test
      {:stream_data, "~> 1.0", only: [:test]},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.30", only: [:dev, :test], runtime: false},
      {:earmark, "~> 1.4", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:igniter, "~> 0.7", only: [:dev, :test]},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false}
    ]
  end
end
