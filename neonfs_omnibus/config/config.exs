import Config

config :logger, :default_handler, formatter: Logger.Formatter.new(metadata: :all)

if Mix.env() == :test do
  config :logger, level: :warning

  config :ra,
    data_dir: ~c"/tmp/neonfs_omnibus_test/ra"

  config :neonfs_core,
    blob_store_base_dir: "/tmp/neonfs_omnibus_test/blobs",
    drives: [%{id: "default", path: "/tmp/neonfs_omnibus_test/blobs", tier: :hot, capacity: 0}],
    meta_dir: "/tmp/neonfs_omnibus_test/meta",
    metrics_enabled: false,
    ra_data_dir: "/tmp/neonfs_omnibus_test/ra",
    snapshot_interval_ms: 100,
    enable_ra: true,
    start_children?: false

  config :neonfs_fuse, start_supervisor: false
  config :neonfs_nfs, start_supervisor: false
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
