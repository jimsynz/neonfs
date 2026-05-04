defmodule NeonFS.WebDAV.MixProject do
  use Mix.Project

  @moduledoc """
  WebDAV interface for NeonFS.
  """
  @version "0.2.3"

  def project do
    [
      aliases: aliases(),
      app: :neonfs_webdav,
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

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {NeonFS.WebDAV.Application, []}
    ]
  end

  defp releases do
    [
      neonfs_webdav: [
        include_executables_for: [:unix],
        steps: [:assemble, :tar]
      ]
    ]
  end

  defp aliases, do: []
  defp docs, do: [main: "readme", extras: ["README.md"]]
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:bandit, "~> 1.5"},
      {:logger_json, "~> 7.0"},
      {:neonfs_client, path: "../neonfs_client"},
      {:plug, "~> 1.15"},
      {:davy, "~> 0.3.0"},
      {:telemetry_metrics_prometheus_core, "~> 1.2"},

      # dev/test
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.22", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:earmark, "~> 1.4", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:igniter, "~> 0.7", only: [:dev, :test]},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false},

      # test
      {:neonfs_test_support, path: "../neonfs_test_support", only: :test, runtime: false},
      {:mimic, "~> 2.0", only: [:test]},
      {:req, "~> 0.5", only: [:dev, :test]}
    ]
  end
end
