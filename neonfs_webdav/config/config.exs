import Config

# Custom metadata keys for structured logging (NeonFS.Core.Log).
#
# `:default_formatter` is the only place these belong. Logger normalises it into
# the default handler's formatter, and Credo's MissedMetadataKeyInLoggerConfig
# check reads it. Do not also set `:default_handler` here: it silently wins,
# leaving this list decorative, so a key declared here and missing from that
# copy is dropped at render while Credo reports it configured. Production still
# overrides the handler from runtime.exs with LoggerJSON.Formatters.Basic.
config :logger, :default_formatter,
  metadata: [
    :chunk_hash,
    :component,
    :core_node,
    :method,
    :module,
    :node_name,
    :operation,
    :path,
    :port,
    :reason,
    :request_id,
    :volume
  ]

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
  config :neonfs_webdav, start_supervisor: false

  config :logger, level: :warning
end
