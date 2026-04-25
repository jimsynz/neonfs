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

if Mix.env() == :test do
  config :neonfs_csi, start_supervisor: false
end

if Mix.env() in [:dev, :test] do
  config :git_ops,
    mix_project: Mix.Project.get!(),
    types: [tidbit: [hidden?: true], important: [header: "Important Changes"]],
    manage_mix_version?: true,
    manage_readme_version: false,
    version_tag_prefix: "v"
end
