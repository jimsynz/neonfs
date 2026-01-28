defmodule NeonFS.Core.RpcServerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.VolumeRegistry

  # Use the default port that the application RPC server is listening on
  @test_port 4370

  setup do
    # Set cookie for tests
    cookie = "test_cookie_#{:rand.uniform(1_000_000)}"
    System.put_env("RELEASE_COOKIE", cookie)

    # Clear volume registry
    if :ets.whereis(:volumes_by_id) do
      :ets.delete_all_objects(:volumes_by_id)
      :ets.delete_all_objects(:volumes_by_name)
    end

    # The RPC server is already started by the application
    # Wait a bit to ensure it's ready
    Process.sleep(50)

    on_exit(fn ->
      System.delete_env("RELEASE_COOKIE")

      # Clear volumes created during test
      if :ets.whereis(:volumes_by_id) do
        :ets.delete_all_objects(:volumes_by_id)
        :ets.delete_all_objects(:volumes_by_name)
      end
    end)

    %{cookie: cookie}
  end

  describe "server startup" do
    test "server is running", %{cookie: _cookie} do
      assert Process.whereis(NeonFS.Core.RpcServer) != nil
    end

    test "listens on configured port", %{cookie: _cookie} do
      {:ok, socket} = :gen_tcp.connect(~c"localhost", @test_port, [:binary, active: false])
      :gen_tcp.close(socket)
    end
  end

  describe "RPC request handling" do
    test "cluster_status returns cluster information", %{cookie: cookie} do
      result = call_rpc(cookie, "Elixir.NeonFS.CLI.Handler", "cluster_status", [])

      assert {:ok, info} = result
      assert is_map(info)
      assert Map.has_key?(info, :name)
      assert Map.has_key?(info, :status)
    end

    test "list_volumes returns empty list initially", %{cookie: cookie} do
      result = call_rpc(cookie, "Elixir.NeonFS.CLI.Handler", "list_volumes", [])

      assert {:ok, []} = result
    end

    test "create_volume with ETF map", %{cookie: cookie} do
      # CLI sends ETF with string keys
      config = %{
        "durability" => %{
          "type" => :replicate,
          "factor" => 3,
          "min_copies" => 2
        }
      }

      result =
        call_rpc(cookie, "Elixir.NeonFS.CLI.Handler", "create_volume", ["test-vol", config])

      assert {:ok, volume} = result
      assert is_map(volume)
      # Handler returns map with atom keys for ETF encoding to CLI
      assert volume[:name] == "test-vol"
    end

    test "get_volume returns volume info", %{cookie: cookie} do
      # Create a volume first
      VolumeRegistry.create("test-vol",
        durability: %{type: :replicate, factor: 1, min_copies: 1}
      )

      result = call_rpc(cookie, "Elixir.NeonFS.CLI.Handler", "get_volume", ["test-vol"])

      assert {:ok, volume} = result
      assert volume.name == "test-vol"
    end

    test "delete_volume removes volume", %{cookie: cookie} do
      # Create a volume first
      VolumeRegistry.create("test-vol",
        durability: %{type: :replicate, factor: 1, min_copies: 1}
      )

      result = call_rpc(cookie, "Elixir.NeonFS.CLI.Handler", "delete_volume", ["test-vol"])

      assert {:ok, %{}} = result
      assert {:error, _} = VolumeRegistry.get_by_name("test-vol")
    end

    test "mount returns error when FUSE not available", %{cookie: cookie} do
      result = call_rpc(cookie, "Elixir.NeonFS.CLI.Handler", "mount", ["vol", "/mnt", %{}])

      assert {:error, :fuse_not_available} = result
    end

    test "list_mounts returns error when FUSE not available", %{cookie: cookie} do
      result = call_rpc(cookie, "Elixir.NeonFS.CLI.Handler", "list_mounts", [])

      assert {:error, :fuse_not_available} = result
    end

    test "unknown function returns error", %{cookie: cookie} do
      result = call_rpc(cookie, "Elixir.NeonFS.CLI.Handler", "unknown_function", [])

      assert {:error, "unknown_function"} = result
    end
  end

  describe "authentication" do
    test "invalid cookie is rejected", %{cookie: _cookie} do
      result = call_rpc("wrong_cookie", "Elixir.NeonFS.CLI.Handler", "cluster_status", [])

      assert {:error, "invalid_cookie"} = result
    end

    test "malformed request is rejected", %{cookie: _cookie} do
      {:ok, socket} =
        :gen_tcp.connect(~c"localhost", @test_port, [:binary, packet: 4, active: false])

      # Send malformed request (not a tuple)
      request = :erlang.term_to_binary("not a tuple")
      :ok = :gen_tcp.send(socket, request)

      {:ok, response} = :gen_tcp.recv(socket, 0, 1000)
      result = :erlang.binary_to_term(response)

      assert {:error, "invalid_request"} = result

      :gen_tcp.close(socket)
    end
  end

  describe "concurrent connections" do
    test "handles multiple concurrent clients", %{cookie: cookie} do
      tasks =
        for _ <- 1..5 do
          Task.async(fn ->
            call_rpc(cookie, "Elixir.NeonFS.CLI.Handler", "cluster_status", [])
          end)
        end

      results = Task.await_many(tasks)

      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)
    end
  end

  # Helper to make RPC calls
  defp call_rpc(cookie, module, function, args) do
    {:ok, socket} =
      :gen_tcp.connect(~c"localhost", @test_port, [:binary, packet: 4, active: false])

    request = {:rpc, cookie, module, function, args}
    request_data = :erlang.term_to_binary(request)

    :ok = :gen_tcp.send(socket, request_data)
    {:ok, response_data} = :gen_tcp.recv(socket, 0, 5000)

    :gen_tcp.close(socket)

    :erlang.binary_to_term(response_data)
  end
end
