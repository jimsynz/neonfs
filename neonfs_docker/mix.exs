defmodule NeonFS.Docker.MixProject do
  use Mix.Project

  @moduledoc """
  Docker / Podman VolumeDriver plugin for NeonFS.

  Exposes the Docker Volume Plugin HTTP protocol over a Unix socket,
  mapping plugin volumes onto NeonFS volumes via `NeonFS.Client.Router`.
  """
  @version "0.3.1"

  def project do
    [
      aliases: aliases(),
      app: :neonfs_docker,
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
      mod: {NeonFS.Docker.Application, []}
    ]
  end

  defp releases do
    [
      neonfs_docker: [
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
      {:bandit, "~> 1.5"},
      {:jason, "~> 1.4"},
      {:neonfs_client, path: "../neonfs_client"},
      {:plug, "~> 1.15"},

      # dev/test
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.22", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:earmark, "~> 1.4", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:igniter, "~> 0.8", only: [:dev, :test]},
      {:mimic, "~> 2.0", only: [:test]},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false},
      {:neonfs_test_support, path: "../neonfs_test_support", only: :test, runtime: false}
    ]
  end
end
