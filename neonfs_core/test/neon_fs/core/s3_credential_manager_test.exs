defmodule NeonFS.Core.S3CredentialManagerTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{RaServer, S3CredentialManager}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  describe "create/1" do
    test "generates a credential with access key and secret" do
      assert {:ok, credential} = S3CredentialManager.create(%{user: "alice"})

      assert String.starts_with?(credential.access_key_id, "NEONFS")
      assert byte_size(credential.access_key_id) == 20
      assert is_binary(credential.secret_access_key)
      assert byte_size(credential.secret_access_key) > 20
      assert credential.identity == %{user: "alice"}
      assert %DateTime{} = credential.created_at
    end

    test "generates unique credentials each time" do
      {:ok, cred1} = S3CredentialManager.create(%{user: "alice"})
      {:ok, cred2} = S3CredentialManager.create(%{user: "alice"})

      refute cred1.access_key_id == cred2.access_key_id
      refute cred1.secret_access_key == cred2.secret_access_key
    end
  end

  describe "lookup/1" do
    test "returns credential for known access key" do
      {:ok, created} = S3CredentialManager.create(%{user: "bob"})

      assert {:ok, found} = S3CredentialManager.lookup(created.access_key_id)
      assert found.access_key_id == created.access_key_id
      assert found.secret_access_key == created.secret_access_key
      assert found.identity == %{user: "bob"}
    end

    test "returns not_found for unknown access key" do
      assert {:error, :not_found} = S3CredentialManager.lookup("NEONFS_NONEXISTENT")
    end
  end

  describe "delete/1" do
    test "removes a credential" do
      {:ok, cred} = S3CredentialManager.create(%{user: "charlie"})

      assert :ok = S3CredentialManager.delete(cred.access_key_id)
      assert {:error, :not_found} = S3CredentialManager.lookup(cred.access_key_id)
    end

    test "returns not_found for unknown access key" do
      assert {:error, :not_found} = S3CredentialManager.delete("NEONFS_NONEXISTENT")
    end
  end

  describe "rotate/1" do
    test "generates a new secret key while keeping the same access key ID" do
      {:ok, original} = S3CredentialManager.create(%{user: "dave"})

      assert {:ok, rotated} = S3CredentialManager.rotate(original.access_key_id)

      assert rotated.access_key_id == original.access_key_id
      assert rotated.identity == original.identity
      refute rotated.secret_access_key == original.secret_access_key
      assert is_binary(rotated.secret_access_key)
      assert byte_size(rotated.secret_access_key) > 20
    end

    test "rotated credential can be looked up with the new secret" do
      {:ok, original} = S3CredentialManager.create(%{user: "eve"})
      {:ok, rotated} = S3CredentialManager.rotate(original.access_key_id)

      {:ok, found} = S3CredentialManager.lookup(original.access_key_id)
      assert found.secret_access_key == rotated.secret_access_key
    end

    test "returns not_found for unknown access key" do
      assert {:error, :not_found} = S3CredentialManager.rotate("NEONFS_NONEXISTENT")
    end
  end

  describe "list/1" do
    # Ra persists to disk across tests within a module (see
    # Codebase-Patterns wiki), so each test namespaces its identity to
    # make assertions robust against leftover state from earlier tests.
    test "returns credentials without secrets, filtered by identity" do
      id1 = %{user: "list_#{System.unique_integer([:positive])}_a"}
      id2 = %{user: "list_#{System.unique_integer([:positive])}_b"}

      {:ok, _} = S3CredentialManager.create(id1)
      {:ok, _} = S3CredentialManager.create(id2)

      listed = S3CredentialManager.list(identity: id1)

      assert length(listed) == 1
      assert Enum.all?(listed, fn c -> c.identity == id1 end)
      assert Enum.all?(listed, fn c -> not Map.has_key?(c, :secret_access_key) end)
    end

    test "filters by identity and returns multiple matching entries" do
      identity = %{user: "list_#{System.unique_integer([:positive])}"}
      {:ok, _} = S3CredentialManager.create(identity)
      {:ok, _} = S3CredentialManager.create(identity)
      {:ok, _} = S3CredentialManager.create(%{user: "someone_else"})

      listed = S3CredentialManager.list(identity: identity)

      assert length(listed) == 2
      assert Enum.all?(listed, fn c -> c.identity == identity end)
    end

    test "filter by unknown identity returns an empty list" do
      unknown = %{user: "list_unknown_#{System.unique_integer([:positive])}"}
      assert [] = S3CredentialManager.list(identity: unknown)
    end
  end

  describe "Ra unavailable" do
    setup do
      stop_ra()
      :ok
    end

    test "writes return {:error, :ra_not_available}" do
      assert {:error, :ra_not_available} = S3CredentialManager.create(%{user: "alice"})
      assert {:error, :not_found} = S3CredentialManager.delete("NEONFS_UNKNOWN")
      assert {:error, :not_found} = S3CredentialManager.rotate("NEONFS_UNKNOWN")
    end

    test "reads return :not_found / empty when the state is unreachable" do
      assert {:error, :not_found} = S3CredentialManager.lookup("NEONFS_UNKNOWN")
      assert [] = S3CredentialManager.list()
    end
  end
end
