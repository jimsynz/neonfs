defmodule NeonFS.Containerd.MixProject do
  use Mix.Project

  @moduledoc """
  containerd content store gRPC plugin for NeonFS.

  Speaks the `containerd.services.content.v1.Content` gRPC protocol
  over a Unix domain socket so containerd's `[proxy_plugins]` config
  can dial NeonFS as a content store.

  This package provides the scaffold (#548). The streaming Read /
  Write RPCs land in #549 / #550 and the metadata Info / List /
  Update / Delete RPCs in #551 — see the `#196` tracking issue for
  the full breakdown.
  """
  @version "0.3.0"

  def project do
    [
      aliases: aliases(),
      app: :neonfs_containerd,
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
      mod: {NeonFS.Containerd.Application, []}
    ]
  end

  defp releases do
    [
      neonfs_containerd: [
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
      {:grpc, "~> 0.9"},
      {:neonfs_client, path: "../neonfs_client"},
      {:protobuf, "~> 0.12"},

      # dev/test
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.22", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:earmark, "~> 1.4", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:mimic, "~> 2.0", only: [:test]},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false}
    ]
  end
end
