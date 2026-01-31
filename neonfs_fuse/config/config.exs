import Config

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
  config :neonfs_fuse, start_supervisor: false
end
