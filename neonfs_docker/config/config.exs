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
end

if Mix.env() in [:dev, :test] do
  config :git_ops,
    mix_project: Mix.Project.get!(),
    types: [tidbit: [hidden?: true], important: [header: "Important Changes"]],
    manage_mix_version?: true,
    manage_readme_version: false,
    version_tag_prefix: "v"
end
