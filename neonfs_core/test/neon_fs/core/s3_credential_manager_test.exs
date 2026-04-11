defmodule NeonFS.Core.S3CredentialManagerTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.S3CredentialManager

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_s3_credential_manager()
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

  describe "list/1" do
    test "returns all credentials without secrets" do
      {:ok, _} = S3CredentialManager.create(%{user: "alice"})
      {:ok, _} = S3CredentialManager.create(%{user: "bob"})

      credentials = S3CredentialManager.list()

      assert length(credentials) == 2
      assert Enum.all?(credentials, fn c -> not Map.has_key?(c, :secret_access_key) end)
    end

    test "filters by identity" do
      {:ok, _} = S3CredentialManager.create(%{user: "alice"})
      {:ok, _} = S3CredentialManager.create(%{user: "alice"})
      {:ok, _} = S3CredentialManager.create(%{user: "bob"})

      alice_creds = S3CredentialManager.list(identity: %{user: "alice"})
      assert length(alice_creds) == 2
      assert Enum.all?(alice_creds, fn c -> c.identity == %{user: "alice"} end)
    end

    test "returns empty list when no credentials exist" do
      assert S3CredentialManager.list() == []
    end
  end
end
