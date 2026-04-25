defmodule NeonfsApi.MixProject do
  use Mix.Project

  @moduledoc """
  Ash-based management API.
  """
  @version "0.1.0"

  def project do
    [
      aliases: aliases(),
      app: :neonfs_api,
      consolidate_protocols: Mix.env() != :dev,
      deps: deps(),
      description: @moduledoc,
      docs: docs(),
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      releases: releases(),
      start_permanent: Mix.env() == :prod,
      usage_rules: usage_rules(),
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
      extra_applications: [:logger]
    ]
  end

  defp releases do
    [
      neonfs_api: [
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
      {:argon2_elixir, "~> 4.0"},
      {:ash, "~> 3.0"},
      {:ash_authentication, "~> 5.0-rc"},
      {:ash_graphql, "~> 1.0"},
      {:bandit, "~> 1.5"},
      {:neonfs_client, path: "../neonfs_client"},
      {:picosat_elixir, "~> 0.2"},
      {:plug, "~> 1.15"},
      {:telemetry_metrics_prometheus_core, "~> 1.2"},

      # dev/test
      {:sourceror, "~> 1.8", only: [:dev, :test]},
      {:usage_rules, "~> 1.0", only: [:dev]}
    ]
  end

  defp usage_rules do
    [
      file: "CLAUDE.md",
      usage_rules: ["usage_rules:all"],
      skills: [
        location: ".claude/skills",
        builds: [
          "ash-framework": [
            description:
              "Use this skill working with Ash Framework or any of its extensions. Always consult this when making an domain changes, features or fixes.",
            usage_rules: [:ash, ~r/^ash_/]
          ]
        ]
      ]
    ]
  end
end
