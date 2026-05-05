defmodule NeonFS.Omnibus.MixProject do
  use Mix.Project

  @moduledoc """
  All-in-one NeonFS deployment combining core, FUSE, NFS, S3, WebDAV, Docker, and containerd services.
  """
  @version "0.2.4"

  def project do
    [
      aliases: aliases(),
      app: :neonfs_omnibus,
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
      extra_applications: [:logger, :crypto],
      mod: {NeonFS.Omnibus.Application, []}
    ]
  end

  defp releases do
    [
      neonfs_omnibus: [
        include_executables_for: [:unix],
        steps: [:assemble, :tar],
        applications: [
          neonfs_fuse: :load,
          neonfs_nfs: :load,
          neonfs_s3: :load,
          neonfs_webdav: :load,
          neonfs_docker: :load,
          neonfs_containerd: :load
        ]
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
      {:neonfs_core, path: "../neonfs_core"},
      {:neonfs_fuse, path: "../neonfs_fuse", runtime: false},
      {:neonfs_nfs, path: "../neonfs_nfs", runtime: false},
      {:neonfs_s3, path: "../neonfs_s3", runtime: false},
      {:neonfs_webdav, path: "../neonfs_webdav", runtime: false},
      {:neonfs_docker, path: "../neonfs_docker", runtime: false},
      {:neonfs_containerd, path: "../neonfs_containerd", runtime: false},

      # dev/test
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.22", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:earmark, "~> 1.4", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:igniter, "~> 0.7", only: [:dev, :test]},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false}
    ]
  end
end
