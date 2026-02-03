import Config

# Test environment configuration
if Mix.env() == :test do
  config :neonfs_core,
    blob_store_base_dir: "/tmp/neonfs_test/blobs",
    meta_dir: "/tmp/neonfs_test/meta",
    ra_data_dir: "/tmp/neonfs_test/ra",
    snapshot_interval_ms: 100,
    enable_ra: true,
    # Don't auto-start children in tests - each test starts what it needs
    start_children?: false
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
