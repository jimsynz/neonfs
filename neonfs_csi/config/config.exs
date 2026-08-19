import Config

config :logger, :default_formatter,
  metadata: [
    :component,
    :core_node,
    :driver_name,
    :mode,
    :module,
    :node_name,
    :operation,
    :port,
    :reason,
    :request_id,
    :rpc,
    :volume_id
  ]

# The node plugin mounts in its own pod, so the CSI release carries
# `neonfs_fuse` — but only for its mount stack. Letting that application's
# own supervisor start would register a phantom `:fuse` service in the cluster
# registry and open a second metrics listener whose port would have to be kept
# off the plugin's; `NeonFS.CSI.Supervisor` starts the mount stack it actually
# needs instead.
config :neonfs_fuse, start_supervisor: false

if Mix.env() == :test do
  config :neonfs_csi, start_supervisor: false

  # A node-mode supervisor starts the `neonfs_fuse` mount stack, which
  # reconciles against its on-disk mount registry at boot. Point that at a
  # temp file so the suite neither reads nor writes `/var/lib/neonfs`.
  config :neonfs_fuse,
    mount_recovery_attempts: 1,
    mount_registry_path: Path.join(System.tmp_dir!(), "neonfs_csi_test_fuse_mounts.json")
end

if Mix.env() in [:dev, :test] do
  config :git_ops,
    mix_project: Mix.Project.get!(),
    types: [tidbit: [hidden?: true], important: [header: "Important Changes"]],
    manage_mix_version?: true,
    manage_readme_version: false,
    version_tag_prefix: "v"
end
