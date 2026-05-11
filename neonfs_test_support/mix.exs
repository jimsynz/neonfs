defmodule NeonFS.TestSupport.MixProject do
  use Mix.Project

  @moduledoc """
  Shared peer-cluster scaffolding and profiling helpers for
  cross-package integration tests. Used by `neonfs_integration`,
  `neonfs_fuse`, `neonfs_s3`, `neonfs_webdav`, `neonfs_docker`, and
  any future interface package that needs a multi-node test harness.

  See sub-issue #599 (foundation for #582).
  """
  @version "0.3.2"

  def project do
    [
      app: :neonfs_test_support,
      deps: deps(),
      description: @moduledoc,
      dialyzer: dialyzer(),
      docs: docs(),
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      version: @version
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto, :ex_unit]
    ]
  end

  defp dialyzer do
    [
      ignore_warnings: ".dialyzer_ignore.exs",
      plt_add_apps: [
        :bandit,
        :ex_unit,
        :neonfs_client,
        :neonfs_core,
        :thousand_island
      ]
    ]
  end

  defp docs, do: [main: "readme", extras: ["README.md"]]
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:neonfs_client, path: "../neonfs_client"},
      # `neonfs_core` is a transitive concern — `ClusterCase`,
      # `EventCollector`, and `TelemetryForwarder` reference modules
      # in the control plane (e.g. `NeonFS.Core.RaSupervisor`,
      # `NeonFS.Events.Envelope`) that consumers always have on their
      # code path. Pulling it in here keeps Dialyzer + xref happy
      # without re-declaring the dep in every consumer.
      {:neonfs_core, path: "../neonfs_core", runtime: false},
      # `bandit` + `thousand_island` are referenced by `ClusterCase`'s
      # HTTP-server retry helper. Same rationale: consumers that boot
      # an HTTP listener have them as runtime deps; testing this
      # package's xref in isolation should still compile cleanly.
      # The version constraints match `neonfs_core`'s declarations.
      {:bandit, "~> 1.5"},
      {:thousand_island, "~> 1.0"},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.21", only: [:dev, :test], runtime: false},
      {:earmark, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.30", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false}
    ]
  end
end
