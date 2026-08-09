import Config

config :logger, :default_formatter,
  metadata: [
    :component,
    :core_node,
    :device,
    :module,
    :node_name,
    :operation,
    :port,
    :reason,
    :volume_name
  ]

if Mix.env() in [:dev, :test] do
  config :git_ops,
    mix_project: Mix.Project.get!(),
    types: [tidbit: [hidden?: true], important: [header: "Important Changes"]],
    manage_mix_version?: true,
    manage_readme_version: false,
    version_tag_prefix: "v"
end
