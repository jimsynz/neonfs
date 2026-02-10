import Config

# Suppress log output during tests (ExUnit's capture_log handles test-specific logs)
if Mix.env() == :test do
  config :logger, level: :warning

  # Don't start neonfs_core children on the test controller —
  # only peer nodes should run MetadataStore, ChunkIndex, etc.
  # Without this, the test controller is included in the quorum ring
  # and causes 4-node rings instead of the expected 3.
  config :neonfs_core, start_children?: false
end

if Mix.env() in [:dev, :test] do
  config :git_ops,
    mix_project: Mix.Project.get!(),
    types: [tidbit: [hidden?: true], important: [header: "Important Changes"]],
    github_handle_lookup?: false,
    version_tag_prefix: "v",
    manage_mix_version?: true,
    manage_readme_version: true
end
