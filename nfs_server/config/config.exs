import Config

# Register custom metadata keys for structured logging in this
# library. Consumers can extend this list in their own config.
config :logger, :default_formatter, metadata: [:bind, :kind, :port, :reason]

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
  config :logger, level: :warning
end
