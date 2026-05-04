defmodule NeonFS.S3.MixProject do
  use Mix.Project

  @moduledoc """
  S3-compatible API interface for NeonFS.
  """
  @version "0.2.2"

  def project do
    [
      aliases: aliases(),
      app: :neonfs_s3,
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
      # `:neonfs_webdav` is a test-only path dep used by
      # `streaming_upload_peak_rss_test.exs` to verify cross-protocol
      # byte-identity. PLT it explicitly so dialyzer can see
      # `NeonFS.WebDAV.Backend.{put_content_stream,resolve,get_content}/*`.
      plt_add_apps: [:neonfs_webdav]
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
      mod: {NeonFS.S3.Application, []}
    ]
  end

  defp releases do
    [
      neonfs_s3: [
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
      {:firkin, "~> 0.2.1"},
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
      # `:neonfs_webdav` is needed by `streaming_upload_peak_rss_test.exs`
      # (and its helper `streaming_test_helpers.ex`) to verify
      # cross-protocol byte identity — the test writes through S3
      # and reads back through WebDAV. The test moved here from
      # `neonfs_integration` under #604; if a more idiomatic home
      # for cross-protocol assertions emerges, this dep is the
      # natural follow-up to revisit.
      {:neonfs_webdav, path: "../neonfs_webdav", only: :test, runtime: false},
      {:ex_aws, "~> 2.6", only: :test},
      {:ex_aws_s3, "~> 2.5", only: :test},
      {:hackney, "~> 1.16", only: :test},
      # `:only` dropped because `rustler` — pulled in transitively via
      # neonfs_client's chunker NIF (#449) — declares jason as a
      # runtime dep. Narrower `:only` bounds here fail the mix.exs
      # consistency check (same shape as the `stream_data` / `ash`
      # gotcha in the Codebase Patterns wiki).
      {:jason, "~> 1.0"},
      {:mimic, "~> 2.0", only: [:test]},
      {:sweet_xml, "~> 0.7", only: :test}
    ]
  end
end
