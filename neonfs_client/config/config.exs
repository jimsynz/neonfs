import Config

# Register custom metadata keys for structured logging.
# The :default_formatter config is read by Credo's MissedMetadataKeyInLoggerConfig check.
config :logger, :default_formatter,
  metadata: [
    :attempt,
    :component,
    :core_node,
    :count,
    :days_remaining,
    :new_expiry,
    :node,
    :node_name,
    :num_acceptors,
    :old_expiry,
    :port,
    :reason,
    :request_id,
    :retry_minutes,
    :service_type,
    :threshold,
    :volume_id
  ]

if Mix.env() == :test do
  config :neonfs_client, start_children?: false
end
