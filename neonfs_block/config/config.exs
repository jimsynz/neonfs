import Config

# Private by default per AGENTS.md's listener posture: NBD has no
# authentication, so anything that can reach the port can attach any export
# this node resolves. Container images widen the bind deliberately.
config :neonfs_block,
  bind: "127.0.0.1",
  port: 10_809

config :logger, :default_formatter,
  metadata: [
    :command,
    :component,
    :core_node,
    :current_epoch,
    :device,
    :export,
    :module,
    :node_name,
    :offset,
    :operation,
    :port,
    :reason,
    :volume_name
  ]

if Mix.env() == :test do
  # Tests start their own listener on an ephemeral port; the configured one
  # would collide between them and with anything already using 10809.
  config :neonfs_block, start_supervisor: false, register_service: false
end

if Mix.env() in [:dev, :test] do
  config :git_ops,
    mix_project: Mix.Project.get!(),
    types: [tidbit: [hidden?: true], important: [header: "Important Changes"]],
    manage_mix_version?: true,
    manage_readme_version: false,
    version_tag_prefix: "v"
end
