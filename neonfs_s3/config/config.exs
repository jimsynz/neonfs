import Config

config :logger, :default_formatter,
  metadata: [
    :bucket,
    :chunk_hash,
    :component,
    :core_node,
    :key,
    :module,
    :node_name,
    :operation,
    :port,
    :reason,
    :request_id,
    :upload_id
  ]

config :logger, :default_handler,
  formatter:
    Logger.Formatter.new(
      metadata: [
        :bucket,
        :component,
        :core_node,
        :key,
        :module,
        :node_name,
        :operation,
        :port,
        :reason,
        :request_id,
        :upload_id
      ]
    )

if Mix.env() in [:dev, :test] do
  config :git_ops,
    mix_project: Mix.Project.get!(),
    types: [tidbit: [hidden?: true], important: [header: "Important Changes"]],
    github_handle_lookup?: false,
    version_tag_prefix: "v",
    manage_mix_version?: true,
    manage_readme_version: true
end

if Mix.env() == :test do
  config :neonfs_client, start_children?: false
  config :neonfs_s3, start_supervisor: false

  config :logger, level: :warning
end
