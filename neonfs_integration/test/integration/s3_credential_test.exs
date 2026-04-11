defmodule NeonFS.Integration.S3CredentialTest do
  @moduledoc """
  Integration tests for S3 credential management across a real multi-node cluster.

  Verifies that credentials created on one core node are accessible from all
  nodes, survive the credential manager's ETS+Ra backing, and that the S3
  authentication flow works end-to-end with real credentials.
  """
  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Integration.S3CoreBridge
  alias NeonFS.S3.MultipartStore

  @moduletag timeout: 180_000
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    init_multi_node_cluster(cluster, name: "s3-cred-test")

    node1 = PeerCluster.get_node!(cluster, :node1)
    S3CoreBridge.store_core_node(node1.node)

    on_exit(fn ->
      S3CoreBridge.cleanup()
    end)

    %{}
  end

  describe "credential CRUD via core RPC" do
    test "create credential returns access key and secret", %{cluster: cluster} do
      {:ok, credential} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :create, [
          %{user: "crud-test"}
        ])

      assert String.starts_with?(credential.access_key_id, "NEONFS")
      assert byte_size(credential.access_key_id) == 20
      assert is_binary(credential.secret_access_key)
      assert credential.identity == %{user: "crud-test"}
    end

    test "lookup credential returns matching secret", %{cluster: cluster} do
      {:ok, created} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :create, [
          %{user: "lookup-test"}
        ])

      {:ok, found} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :lookup, [
          created.access_key_id
        ])

      assert found.access_key_id == created.access_key_id
      assert found.secret_access_key == created.secret_access_key
      assert found.identity == %{user: "lookup-test"}
    end

    test "lookup via NeonFS.Core facade returns expected shape", %{cluster: cluster} do
      {:ok, created} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :create, [
          %{user: "facade-test"}
        ])

      {:ok, result} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core, :lookup_s3_credential, [
          created.access_key_id
        ])

      assert result.secret_access_key == created.secret_access_key
      assert result.identity == %{user: "facade-test"}
      assert map_size(result) == 2
    end

    test "lookup unknown credential returns not_found", %{cluster: cluster} do
      assert {:error, :not_found} =
               PeerCluster.rpc(cluster, :node1, NeonFS.Core, :lookup_s3_credential, [
                 "NEONFS_NONEXISTENT"
               ])
    end

    test "list credentials returns all without secrets", %{cluster: cluster} do
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :create, [
          %{user: "list-test-a"}
        ])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :create, [
          %{user: "list-test-b"}
        ])

      creds =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :list, [])

      assert is_list(creds)
      assert length(creds) >= 2

      assert Enum.all?(creds, fn c ->
               not Map.has_key?(c, :secret_access_key)
             end)
    end

    test "delete credential removes it", %{cluster: cluster} do
      {:ok, created} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :create, [
          %{user: "delete-test"}
        ])

      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :delete, [
          created.access_key_id
        ])

      assert {:error, :not_found} =
               PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :lookup, [
                 created.access_key_id
               ])
    end
  end

  describe "cross-node credential access" do
    test "credential created on node1 is accessible from node2", %{cluster: cluster} do
      {:ok, created} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :create, [
          %{user: "cross-node"}
        ])

      {:ok, found} =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core, :lookup_s3_credential, [
          created.access_key_id
        ])

      assert found.secret_access_key == created.secret_access_key
      assert found.identity == %{user: "cross-node"}
    end

    test "credential created on node2 is accessible from node3", %{cluster: cluster} do
      {:ok, created} =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.S3CredentialManager, :create, [
          %{user: "cross-node-2-3"}
        ])

      {:ok, found} =
        PeerCluster.rpc(cluster, :node3, NeonFS.Core, :lookup_s3_credential, [
          created.access_key_id
        ])

      assert found.secret_access_key == created.secret_access_key
    end
  end

  describe "S3 authentication with real credentials" do
    setup %{cluster: cluster} do
      node1 = PeerCluster.get_node!(cluster, :node1)
      {access_key, secret_key} = S3CoreBridge.create_test_credential(node1.node)

      Application.put_env(:neonfs_s3, :core_call_fn, &S3CoreBridge.call/2)

      {:ok, server} =
        Bandit.start_link(
          plug:
            {NeonFS.S3.HealthPlug,
             backend: NeonFS.S3.Backend, core_nodes_fn: fn -> [node1.node] end},
          port: 0,
          ip: :loopback,
          startup_log: false
        )

      {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

      {:ok, multipart_store} = MultipartStore.start_link([])

      config = [
        access_key_id: access_key,
        secret_access_key: secret_key,
        scheme: "http://",
        host: "localhost",
        port: port,
        region: "neonfs",
        s3: [
          scheme: "http://",
          host: "localhost",
          port: port,
          region: "neonfs"
        ]
      ]

      on_exit(fn ->
        try do
          if Process.alive?(multipart_store), do: GenServer.stop(multipart_store, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end

        try do
          Supervisor.stop(server, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end

        Application.delete_env(:neonfs_s3, :core_call_fn)
      end)

      %{config: config, access_key: access_key, secret_key: secret_key}
    end

    test "valid credentials can list buckets", %{config: config} do
      assert {:ok, result} = ExAws.S3.list_buckets() |> ExAws.request(config)
      assert result.status_code == 200
    end

    test "valid credentials can create and read objects", %{config: config} do
      bucket = "s3-cred-auth-#{System.unique_integer([:positive])}"

      ExAws.S3.put_bucket(bucket, "neonfs") |> ExAws.request!(config)

      ExAws.S3.put_object(bucket, "test.txt", "auth works")
      |> ExAws.request!(config)

      result = ExAws.S3.get_object(bucket, "test.txt") |> ExAws.request!(config)
      assert result.body == "auth works"
    end

    test "wrong secret key returns 403", %{config: config} do
      bad_config = Keyword.merge(config, secret_access_key: "wrong-secret")

      assert {:error, {:http_error, 403, _}} =
               ExAws.S3.list_buckets() |> ExAws.request(bad_config)
    end

    test "unknown access key returns 403", %{config: config} do
      bad_config =
        Keyword.merge(config, access_key_id: "NEONFS_UNKNOWN", secret_access_key: "irrelevant")

      assert {:error, {:http_error, 403, _}} =
               ExAws.S3.list_buckets() |> ExAws.request(bad_config)
    end

    test "deleted credential returns 403", %{cluster: cluster, config: config, access_key: key} do
      assert {:ok, _} = ExAws.S3.list_buckets() |> ExAws.request(config)

      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.S3CredentialManager, :delete, [key])

      assert {:error, {:http_error, 403, _}} =
               ExAws.S3.list_buckets() |> ExAws.request(config)
    end
  end
end
