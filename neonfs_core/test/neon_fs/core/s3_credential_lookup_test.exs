defmodule NeonFS.Core.S3CredentialLookupTest do
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

  describe "NeonFS.Core.lookup_s3_credential/1" do
    test "returns secret and identity for known access key" do
      {:ok, created} = S3CredentialManager.create(%{user: "alice"})

      assert {:ok, result} = NeonFS.Core.lookup_s3_credential(created.access_key_id)
      assert result.secret_access_key == created.secret_access_key
      assert result.identity == %{user: "alice"}
    end

    test "returns not_found for unknown access key" do
      assert {:error, :not_found} = NeonFS.Core.lookup_s3_credential("NEONFS_UNKNOWN")
    end

    test "returned map has exactly the fields the S3 backend expects" do
      {:ok, created} = S3CredentialManager.create(%{user: "test"})

      {:ok, result} = NeonFS.Core.lookup_s3_credential(created.access_key_id)

      assert Map.has_key?(result, :secret_access_key)
      assert Map.has_key?(result, :identity)
      assert map_size(result) == 2
    end
  end
end
