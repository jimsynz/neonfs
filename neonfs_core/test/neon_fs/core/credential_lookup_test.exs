defmodule NeonFS.Core.CredentialLookupTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{CredentialManager, RaServer}
  alias NeonFS.Error.NotFound

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    ensure_node_named()
    start_ra()
    :ok = RaServer.init_cluster()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  describe "NeonFS.Core.lookup_credential/1" do
    test "returns secret and identity for known access key" do
      {:ok, created} = CredentialManager.create(%{user: "alice"})

      assert {:ok, result} = NeonFS.Core.lookup_credential(created.access_key_id)
      assert result.secret_access_key == created.secret_access_key
      assert result.identity == %{user: "alice"}
    end

    test "returns not_found for unknown access key" do
      assert {:error, %NotFound{}} = NeonFS.Core.lookup_credential("NEONFS_UNKNOWN")
    end

    # The POSIX identity travels with the secret so that an interface can
    # authorise a request without a second round trip — including on paths
    # where the credential's volume no longer exists.
    test "returned map has exactly the fields the S3 and WebDAV backends expect" do
      {:ok, created} = CredentialManager.create(%{user: "test"}, uid: 1000, gids: [1000, 20])

      {:ok, result} = NeonFS.Core.lookup_credential(created.access_key_id)

      assert Map.has_key?(result, :secret_access_key)
      assert Map.has_key?(result, :identity)
      assert result.uid == 1000
      assert result.gids == [1000, 20]
      assert map_size(result) == 4
    end

    # A credential created without one carries `nil`, not 0. Core reads an
    # absent uid as 0 and `Authorise.check/4` passes 0 unconditionally, so a
    # defaulted uid here would be the root bypass this field exists to close.
    test "a credential created without a uid reports nil rather than 0" do
      {:ok, created} = CredentialManager.create(%{user: "nouid"})

      {:ok, result} = NeonFS.Core.lookup_credential(created.access_key_id)

      assert result.uid == nil
      assert result.gids == []
    end
  end
end
