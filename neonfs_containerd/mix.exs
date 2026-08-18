defmodule NeonFS.Containerd.MixProject do
  use Mix.Project

  @moduledoc """
  containerd content store gRPC plugin for NeonFS.

  Speaks the `containerd.services.content.v1.Content` gRPC protocol
  over a Unix domain socket so containerd's `[proxy_plugins]` config
  can dial NeonFS as a content store.

  Provides the content-store service: streaming Read / Write plus the
  metadata Info / List / Update / Delete RPCs.
  """
  @version "0.6.1"

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
      version: @version,
      hex: [ignore_advisories: ignored_advisories()]
    ]
  end

  # cowlib is transitive (`grpc_server` -> `cowboy` -> `cowlib`) and the newest
  # published release carries these, so there is nothing to upgrade to. Each
  # names a cowlib function this project never reaches: cookie building,
  # structured-header escaping, and `Link` headers — the cowboy here serves
  # gRPC and nothing else. Remove an entry when a fixed cowlib ships; an
  # ignore list nobody revisits is how a real exposure gets inherited.
  defp ignored_advisories do
    [
      # Cookie Request Header Injection in cow_cookie:cookie/1
      "EEF-CVE-2026-43969",
      # HTTP Response Splitting in cow_http_struct_hd:escape_string/2
      "EEF-CVE-2026-43966",
      # Link Header Directive Smuggling in cow_link:link/1
      "EEF-CVE-2026-43971"
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
      {:grpc, "~> 1.0"},
      {:grpc_server, "~> 1.0"},
      {:neonfs_client, path: "../neonfs_client"},
      {:protobuf, "~> 0.17"},

      # dev/test
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.22", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:gun, "~> 2.4", only: [:test]},
      {:mimic, "~> 2.0", only: [:test]},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false}
    ]
  end
end
