defmodule NeonFS.NFS.MixProject do
  use Mix.Project

  @moduledoc """
  NFSv3 server interface for NeonFS.
  """
  @version "0.3.2"

  def project do
    [
      aliases: aliases(),
      app: :neonfs_nfs,
      consolidate_protocols: Mix.env() != :dev,
      deps: deps(),
      description: @moduledoc,
      dialyzer: dialyzer(),
      docs: docs(),
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      releases: releases(),
      start_permanent: Mix.env() == :prod,
      version: @version
    ]
  end

  defp dialyzer do
    [
      # `:neonfs_core` is a test-only transitive dep (via
      # `:neonfs_test_support`) used by the BEAM NFSv3 read-path
      # smoke test (#587). PLT it explicitly so dialyzer can see
      # `NeonFS.Core.read_file_stream/3` referenced by
      # `test/support/beam_read_test_hooks.ex`.
      plt_add_apps: [:neonfs_core]
    ]
  end

  defp package do
    [
      maintainers: ["James Harton <james@harton.dev>"],
      licenses: ["Apache-2.0"]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {NeonFS.NFS.Application, []}
    ]
  end

  defp releases do
    [
      neonfs_nfs: [
        include_executables_for: [:unix],
        steps: [:assemble, :tar]
      ]
    ]
  end

  defp aliases, do: []
  defp docs, do: [main: "readme", extras: ["README.md"]]
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:bandit, "~> 1.5"},
      {:logger_json, "~> 7.0"},
      {:neonfs_client, path: "../neonfs_client"},
      {:nfs_server, path: "../nfs_server"},
      {:plug, "~> 1.15"},
      {:telemetry_metrics_prometheus_core, "~> 1.2"},

      # dev/test
      {:mimic, "~> 2.0", only: [:test]},
      {:stream_data, "~> 1.0", only: [:test]},
      # `:neonfs_test_support` pulls `:neonfs_core` transitively for
      # `ClusterCase` / `PeerCluster`. Used by the BEAM NFSv3 read-path
      # smoke test (#587) which boots a 3-peer cluster and drives
      # `Handler.handle_call/4` against a live `NFSv3Backend`.
      {:neonfs_test_support, path: "../neonfs_test_support", only: :test, runtime: false},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.22", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:earmark, "~> 1.4", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:igniter, "~> 0.8", only: [:dev, :test]},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false}
    ]
  end
end
