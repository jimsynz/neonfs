defmodule NeonFS.Integration.MixProject do
  use Mix.Project

  @moduledoc """
  Multi-node integration test suite for NeonFS.
  """
  @version "0.1.11"

  def project do
    [
      app: :neonfs_integration,
      deps: deps(),
      description: @moduledoc,
      dialyzer: dialyzer(),
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      version: @version
    ]
  end

  defp dialyzer do
    [
      ignore_warnings: ".dialyzer_ignore.exs",
      plt_add_apps: [
        :bandit,
        :neonfs_client,
        :neonfs_core,
        :neonfs_docker,
        :neonfs_s3,
        :neonfs_test_support,
        :neonfs_webdav,
        :thousand_island
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # Path dependencies for testing (runtime: false since only used in tests)
      {:neonfs_core, path: "../neonfs_core", runtime: false},
      {:neonfs_docker, path: "../neonfs_docker", runtime: false},
      # `:neonfs_s3` is still needed because `streaming_test_helpers.ex`
      # (a cross-protocol streaming-write helper) and the consuming
      # `streaming_upload_peak_rss_test.exs` reference
      # `NeonFS.S3.Backend` and `Firkin.PutOpts`. Removing the dep
      # cleanly is tracked under #604, alongside the equivalent
      # WebDAV concern.
      {:neonfs_s3, path: "../neonfs_s3", runtime: false},
      {:neonfs_test_support, path: "../neonfs_test_support", runtime: false},
      {:neonfs_webdav, path: "../neonfs_webdav", runtime: false},

      {:jason, "~> 1.0"},

      # WebDAV integration test client
      {:req, "~> 0.5", only: [:dev, :test]},

      # dev/test
      {:stream_data, "~> 1.0"},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.30", only: [:dev, :test], runtime: false},
      {:earmark, "~> 1.4", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:igniter, "~> 0.7", only: [:dev, :test]},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false}
    ]
  end
end
