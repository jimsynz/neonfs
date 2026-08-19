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
    :attempt,
    :atime_mode,
    :bytes,
    :chunk_hash,
    :claim_id,
    :component,
    :core_node,
    :count,
    :days_remaining,
    :file_path,
    :internal_id,
    :kind,
    :mount_id,
    :mount_point,
    :new_expiry,
    :node,
    :node_name,
    :num_acceptors,
    :old_expiry,
    :opcode,
    :operation,
    :port,
    :reason,
    :reply,
    :request_id,
    :retry_minutes,
    :root_inode,
    :scheduler,
    :threshold,
    :volume,
    :volume_id,
    :volume_name,
    :work_id,
    :work_label
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

# In test mode, don't start the supervisor - tests use start_supervised
# for the specific components they need, ensuring proper isolation
if Mix.env() == :test do
  config :neonfs_client, start_children?: false
  config :neonfs_fuse, start_supervisor: false

  # Keep the mount registry out of `/var/lib/neonfs`, which the suite must
  # neither read nor write. Tests that care about its contents override the
  # path per-test; this is the backstop for the ones that only start a
  # `MountManager` and never mount anything.
  config :neonfs_fuse,
    mount_recovery_attempts: 1,
    mount_recovery_backoff_ms: 10,
    mount_registry_path: Path.join(System.tmp_dir!(), "neonfs_fuse_test_mounts.json")

  # Suppress log output during tests (ExUnit's capture_log handles test-specific logs)
  config :logger, level: :warning
end
