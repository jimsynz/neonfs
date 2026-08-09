import Config

repo_root = Path.expand("../../..", __DIR__)

cargo_version_replace = fn version ->
  version = String.trim_leading(version, "v")
  "version = \"#{version}\"\n"
end

config :git_ops,
  mix_project: Mix.Project.get!(),
  changelog_file: Path.join(repo_root, "CHANGELOG.md"),
  github_handle_lookup?: false,
  repository_url: "https://harton.dev/project-neon/neonfs",
  # git_ops defaults this to the cwd and runs `git init` on it, which would
  # create a second, empty repository here rather than reinitialising ours.
  repository_path: repo_root,
  version_tag_prefix: "v",
  managed_files:
    repo_root
    |> Path.join("**/{mix.exs,README.md,Cargo.toml}")
    |> Path.wildcard()
    |> Enum.reject(&String.contains?(&1, ["/deps/", "/_build/", "/target/"]))
    |> Enum.uniq()
    |> Enum.map(fn path ->
      cond do
        String.ends_with?(path, "mix.exs") ->
          {path, :mix}

        String.ends_with?(path, "Cargo.toml") ->
          {path, cargo_version_replace, cargo_version_replace}

        true ->
          {path, :string}
      end
    end)
