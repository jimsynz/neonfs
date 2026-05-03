import Config

cargo_version_replace = fn version ->
  version = String.trim_leading(version, "v")
  "version = \"#{version}\"\n"
end

config :git_ops,
  mix_project: Mix.Project.get!(),
  changelog_file: "CHANGELOG.md",
  github_handle_lookup?: false,
  repository_url: "https://harton.dev/project-neon/neonfs",
  version_tag_prefix: "v",
  managed_files: "*/{mix.exs,README.md,Cargo.toml}"
    |> Path.wildcard()
    |> Enum.concat(["mix.exs"])
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
