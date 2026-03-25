import Config

config :git_ops,
  mix_project: Mix.Project.get!(),
  changelog_file: "CHANGELOG.md",
  github_handle_lookup?: true,
  github_api_base_url: "https://api.github.com",
  repository_url: "https://harton.dev/project-neon/neonfs",
  managed_files: "*/{mix.exs,README.md,Cargo.toml}"
    |> Path.wildcard()
    |> Enum.map(fn path ->
      if String.ends_with?(path, "mix.exs") do
        {path, :mix}
      else
        {path, :string}
      end
    end)
