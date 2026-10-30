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
      # Siblings run on peer nodes rather than here, so none is started
      # (runtime: false). Each is pinned with override: Mix does no version
      # resolution for git deps, so these pins are the combination under test.
      {:neonfs_cifs,
       git: "https://harton.dev/project-neon/neonfs_cifs.git",
       tag: "v0.6.1",
       override: true,
       runtime: false},
      # Fetched for its source only: test_helper.exs builds the CLI binary the
      # CLI tests drive, so the binary is the one pinned here.
      {:neonfs_cli,
       git: "https://harton.dev/project-neon/neonfs_cli.git",
       tag: "v0.6.1",
       override: true,
       compile: false,
       app: false},
      {:neonfs_client,
       git: "https://harton.dev/project-neon/neonfs_client.git",
       tag: "v0.6.1",
       override: true,
       runtime: false},
      {:neonfs_containerd,
       git: "https://harton.dev/project-neon/neonfs_containerd.git",
       tag: "v0.6.1",
       override: true,
       runtime: false},
      {:neonfs_core,
       git: "https://harton.dev/project-neon/neonfs_core.git",
       tag: "v0.6.1",
       override: true,
       runtime: false},
      {:neonfs_csi,
       git: "https://harton.dev/project-neon/neonfs_csi.git",
       tag: "v0.6.1",
       override: true,
       runtime: false},
      {:neonfs_fuse,
       git: "https://harton.dev/project-neon/neonfs_fuse.git",
       tag: "v0.6.1",
       override: true,
       runtime: false},
      {:neonfs_test_support,
       git: "https://harton.dev/project-neon/neonfs_test_support.git",
       tag: "v0.6.1",
       override: true,
       runtime: false},
      {:jason, "~> 1.0"},
      # Declared for the alerts test, which reads it directly. No `:only`:
      # `reactor` (through core) depends on it in every environment, and a
      # narrower one fails Mix's consistency check.
      {:yaml_elixir, "~> 2.11"},

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
    |> use_local_siblings()
  end

  # NeonFS siblings are git deps pinned by tag; `NEONFS_DEPS=local` swaps them
  # for the checkouts in the workspace. This project sits one level down in
  # the `neonfs` repo, so its siblings are two levels up. The tuples above stay
  # literal so Renovate can find and bump them.
  defp use_local_siblings(deps), do: use_local_siblings(deps, System.get_env("NEONFS_DEPS"))
  defp use_local_siblings(deps, "local"), do: Enum.map(deps, &local_sibling/1)
  defp use_local_siblings(deps, _), do: deps

  defp local_sibling({name, [{:git, "https://harton.dev/project-neon/" <> _}, {:tag, _} | opts]}),
    do: {name, [path: "../../#{name}"] ++ opts}

  defp local_sibling(dep), do: dep
end
