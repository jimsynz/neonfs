defmodule NeonFS.Bench.MixProject do
  use Mix.Project

  def project do
    [
      app: :neonfs_bench,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: false,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:benchee, "~> 1.3"},
      {:benchee_json, "~> 1.0"},
      {:benchee_csv, "~> 1.0"},
      {:benchee_html, "~> 1.0"},
      {:jason, "~> 1.4"}
    ]
  end
end
