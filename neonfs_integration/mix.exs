defmodule NeonFS.Integration.MixProject do
  use Mix.Project

  @moduledoc """
  Multi-node integration test suite for NeonFS — narrowed to
  cluster-formation, replication, partition / quorum / failure
  recovery, and the cross-node correctness suites that genuinely
  need a peer cluster spanning multiple core nodes. Per-interface
  tests live with their owning packages and pull peer-cluster
  scaffolding in via `:neonfs_test_support`. See #582 for the
  migration that landed this shape.
  """
  @version "0.2.4"

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
      plt_add_apps: [
        :neonfs_client,
        :neonfs_containerd,
        :neonfs_core,
        :neonfs_test_support
      ]
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
      {:neonfs_containerd, path: "../neonfs_containerd", runtime: false},
      {:neonfs_core, path: "../neonfs_core", runtime: false},
      {:neonfs_test_support, path: "../neonfs_test_support", runtime: false},
      {:jason, "~> 1.0"},

      # dev/test
      {:stream_data, "~> 1.0"},
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
