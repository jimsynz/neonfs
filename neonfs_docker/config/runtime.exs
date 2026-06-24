import Config

if config_env() == :prod do
  config :neonfs_docker,
    drain_deadline_ms:
      String.to_integer(System.get_env("NEONFS_DOCKER_DRAIN_DEADLINE_MS", "25000"))
end
