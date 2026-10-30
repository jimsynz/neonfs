import Config

repo_root = Path.expand("../../..", __DIR__)

# The product repo's own version: what `neonfs-common` and this repo's
# releases carry. The NeonFS packages version themselves in their own repos.
config :git_ops,
  mix_project: Mix.Project.get!(),
  changelog_file: Path.join(repo_root, "CHANGELOG.md"),
  github_handle_lookup?: false,
  repository_url: "https://harton.dev/project-neon/neonfs",
  # git_ops defaults this to the cwd and runs `git init` on it, which would
  # create a second, empty repository here rather than reinitialising ours.
  repository_path: repo_root,
  version_tag_prefix: "v",
  manage_mix_version?: true
