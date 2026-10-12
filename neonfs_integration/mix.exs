defmodule NeonFS.Integration.MixProject do
  use Mix.Project

  @moduledoc """
  Multi-node integration test suite for NeonFS — narrowed to
  cluster-formation, replication, partition / quorum / failure
  recovery, and the cross-node correctness suites that genuinely
  need a peer cluster spanning multiple core nodes. Per-interface
  tests live with their owning packages and pull peer-cluster
  scaffolding in via `:neonfs_test_support`. See that package for the
  migration that landed this shape.
  """
  @version "0.6.1"

  def project do
    [
      app: :neonfs_integration,
      deps: deps(),
      description: @moduledoc,
      dialyzer: dialyzer(),
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      version: @version,
      hex: [ignore_advisories: ignored_advisories()]
    ]
  end

  defp dialyzer do
    [
      ignore_warnings: ".dialyzer_ignore.exs",
      plt_add_apps: [
        :neonfs_cifs,
        :neonfs_client,
        :neonfs_containerd,
        :neonfs_core,
        :neonfs_csi,
        :neonfs_test_support
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto]
    ]
  end

  # cowlib is transitive (`grpc_server` -> `cowboy` -> `cowlib`) and its newest
  # release still carries this, so there is nothing to upgrade to. It names a
  # cowlib function this project never reaches — structured-header escaping,
  # where non-VCHAR bytes survive and a header can carry a response split — and
  # the cowboy here serves gRPC and nothing else. Remove the entry when a fixed
  # cowlib ships; an ignore list nobody revisits is how a real exposure gets
  # inherited.
  defp ignored_advisories do
    [
      # HTTP Response Splitting in cow_http_struct_hd:escape_string/2
      "EEF-CVE-2026-43966"
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # Path dependencies for testing (runtime: false since only used in tests)
      {:neonfs_cifs, path: "../neonfs_cifs", runtime: false},
      {:neonfs_containerd, path: "../neonfs_containerd", runtime: false},
      {:neonfs_core, path: "../neonfs_core", runtime: false},
      {:neonfs_csi, path: "../neonfs_csi", runtime: false},
      {:neonfs_test_support, path: "../neonfs_test_support", runtime: false},
      {:jason, "~> 1.0"},

      # dev/test
      {:stream_data, "~> 1.0"},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_check, "~> 0.17", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.4", only: [:dev, :test], runtime: false},
      {:igniter, "~> 0.8", only: [:dev, :test]},
      {:mix_audit, "~> 2.0", only: [:dev, :test], runtime: false}
    ]
  end
end
