defmodule NeonFS.Block.MixProject do
  use Mix.Project

  @moduledoc """
  Block device target for NeonFS.

  Serves NeonFS volumes as Linux block devices, speaking the Network Block
  Device protocol.
  """
  @version "0.6.1"

  def project do
    [
      aliases: aliases(),
      app: :neonfs_block,
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

  defp package do
    [
      maintainers: ["James Harton <james@harton.dev>"],
      licenses: ["Apache-2.0"]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {NeonFS.Block.Application, []}
    ]
  end

  defp releases do
    [
      neonfs_block: [
        include_executables_for: [:unix],
        steps: [:assemble, :tar]
      ]
    ]
  end

  defp aliases, do: []
  defp dialyzer, do: [ignore_warnings: ".dialyzer_ignore.exs"]
  defp docs, do: [main: "readme", extras: ["README.md"]]
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:neonfs_client, path: "../neonfs_client"},
      {:logger_json, "~> 7.0"},
      {:thousand_island, "~> 1.0"},
      {:telemetry, "~> 1.2"},
      {:telemetry_metrics, "~> 1.0"},
      {:telemetry_metrics_prometheus_core, "~> 1.1"},
      {:bandit, "~> 1.0"},
      {:plug, "~> 1.16"},

      # dev/test
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.22", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end
end
