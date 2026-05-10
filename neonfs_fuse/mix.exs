defmodule NeonFS.FUSE.MixProject do
  use Mix.Project

  @moduledoc """
  FUSE filesystem interface for NeonFS.
  """
  @version "0.2.6"

  def project do
    [
      aliases: aliases(),
      app: :neonfs_fuse,
      consolidate_protocols: Mix.env() != :dev,
      deps: deps(),
      description: @moduledoc,
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

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {NeonFS.FUSE.Application, []}
    ]
  end

  defp releases do
    [
      neonfs_fuse: [
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
      {:fuse_server, path: "../fuse_server"},
      {:logger_json, "~> 7.0"},
      {:neonfs_client, path: "../neonfs_client"},
      {:plug, "~> 1.15"},
      {:telemetry_metrics_prometheus_core, "~> 1.2"},

      # dev/test
      {:neonfs_test_support, path: "../neonfs_test_support", only: :test, runtime: false},
      {:mimic, "~> 2.0", only: [:test]},
      {:stream_data, "~> 1.0", only: [:test]},
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
