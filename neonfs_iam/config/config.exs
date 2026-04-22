import Config

config :neonfs_iam, ash_domains: [NeonFS.IAM]

config :logger, :default_formatter,
  metadata: [
    :component,
    :module,
    :node_name,
    :reason,
    :request_id,
    :user_id
  ]

if Mix.env() in [:dev, :test] do
  config :git_ops,
    mix_project: Mix.Project.get!(),
    types: [tidbit: [hidden?: true], important: [header: "Important Changes"]],
    manage_mix_version?: true,
    manage_readme_version: false,
    version_tag_prefix: "v"
end
