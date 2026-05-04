defmodule NeonFS.IAM.MixProject do
  use Mix.Project

  @moduledoc """
  Identity and access management for NeonFS.

  Hosts the Ash resources (`User`, `Group`, `AccessPolicy`, `IdentityMapping`)
  and the public authentication/authorisation API surface used by the core
  authoriser and the protocol bridges (S3, WebDAV, FUSE, NFS).
  """
  @version "0.2.3"

  def project do
    [
      aliases: aliases(),
      app: :neonfs_iam,
      consolidate_protocols: Mix.env() != :dev,
      deps: deps(),
      description: @moduledoc,
      dialyzer: dialyzer(),
      docs: docs(),
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
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
      mod: {NeonFS.IAM.Application, []}
    ]
  end

  defp aliases, do: []
  defp dialyzer, do: [ignore_warnings: ".dialyzer_ignore.exs"]
  defp docs, do: [main: "readme", extras: ["README.md"]]
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:argon2_elixir, "~> 4.0"},
      {:ash, "~> 3.5"},
      {:ash_authentication, "~> 4.0"},
      {:neonfs_client, path: "../neonfs_client"},

      # dev/test
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.22", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:earmark, "~> 1.4", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:igniter, "~> 0.7", only: [:dev, :test]},
      {:mimic, "~> 2.0", only: [:test]},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.0"},
      {:usage_rules, "~> 1.0", only: [:dev]}
    ]
  end
end
