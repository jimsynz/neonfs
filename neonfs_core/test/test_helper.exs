# Start Erlang distribution for Ra tests (optional)
# Ra requires a named node to work properly
# This will fail in regular test runs and that's OK - Ra tests are optional
case Node.self() do
  :nonode@nohost ->
    # Attempt to start distribution with a test node name
    # Ignore errors if already configured or distribution not available
    case Node.start(:"test@127.0.0.1", :shortnames) do
      {:ok, _} -> :ok
      {:error, _} -> :ok
    end

  _ ->
    # Already running in distributed mode
    :ok
end

# Compile test support modules
Code.require_file("support/test_cluster.ex", __DIR__)
Code.require_file("support/test_helpers.ex", __DIR__)

ExUnit.start()
