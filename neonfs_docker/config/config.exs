import Config

config :logger, :default_formatter,
  metadata: [
    :component,
    :core_node,
    :module,
    :node_name,
    :operation,
    :port,
    :reason,
    :request_id,
    :volume_name
  ]

if Mix.env() == :test do
  # Don't bind the Unix socket or start the registrar in unit tests;
  # individual tests spin up the plug in isolation via Plug.Test.
  config :neonfs_docker, start_supervisor: false

  # Tests that boot a peer cluster `start_supervised!` `Connection`,
  # `Discovery`, and `CostFunction` themselves so they can point at
  # the peer's bootstrap node — otherwise `neonfs_client`'s default
  # children claim those names first and the test setup raises
  # `{:already_started, _}`. Same shape as the s3 / webdav / fuse
  # configs.
  config :neonfs_client, start_children?: false
end

if Mix.env() in [:dev, :test] do
  config :git_ops,
    mix_project: Mix.Project.get!(),
    types: [tidbit: [hidden?: true], important: [header: "Important Changes"]],
    manage_mix_version?: true,
    manage_readme_version: false,
    version_tag_prefix: "v"
end
