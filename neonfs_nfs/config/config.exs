import Config

# Register custom metadata keys for structured logging (NeonFS.Core.Log).
# In production, runtime.exs overrides the handler with LoggerJSON.Formatters.Basic.
# The :default_formatter config is also read by Credo's MissedMetadataKeyInLoggerConfig check.
config :logger, :default_formatter,
  metadata: [
    :bind_address,
    :component,
    :core_node,
    :count,
    :export_id,
    :function,
    :module,
    :name,
    :node,
    :node_name,
    :num_acceptors,
    :operation,
    :port,
    :reason,
    :reply,
    :request_id,
    :root_inode,
    :volume,
    :volume_id,
    :volume_name,
    :work_id,
    :work_label
  ]

config :logger, :default_handler,
  formatter:
    Logger.Formatter.new(
      metadata: [
        :bind_address,
        :component,
        :core_node,
        :count,
        :export_id,
        :function,
        :module,
        :name,
        :node,
        :node_name,
        :num_acceptors,
        :operation,
        :port,
        :reason,
        :request_id,
        :root_inode,
        :volume,
        :volume_id,
        :volume_name,
        :work_id,
        :work_label
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

# In test mode, don't start the supervisor - tests use start_supervised
# for the specific components they need, ensuring proper isolation
if Mix.env() == :test do
  config :neonfs_client, start_children?: false
  config :neonfs_nfs, start_supervisor: false

  # Suppress log output during tests (ExUnit's capture_log handles test-specific logs)
  config :logger, level: :warning
end
