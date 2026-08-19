defmodule NeonFS.Cluster.InviteTest do
  @moduledoc """
  The invite token's shape and its signed redemption budget.

  Enforcement of the budget is `NeonFS.Core.MetadataStateMachine`'s, through
  a Ra apply; what is asserted here is that the budget is carried in the token
  and cannot be edited by whoever holds it.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Cluster.Invite
  alias NeonFS.Cluster.State

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    previous = Application.get_env(:neonfs_core, :meta_dir)
    Application.put_env(:neonfs_core, :meta_dir, tmp_dir)

    :ok =
      State.save(
        State.new(
          "test-cluster-#{System.unique_integer([:positive])}",
          "test-cluster",
          :crypto.strong_rand_bytes(32) |> Base.encode64(),
          %{id: "node-1", name: node(), joined_at: DateTime.utc_now()}
        )
      )

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:neonfs_core, :meta_dir)
        dir -> Application.put_env(:neonfs_core, :meta_dir, dir)
      end
    end)

    :ok
  end

  describe "create_invite/2" do
    test "a token minted without a budget is single-use" do
      assert {:ok, token} = Invite.create_invite(3600)
      assert {:ok, {_random, _expiry, 1, _signature}} = Invite.parse(token)
    end

    test "the requested budget is carried in the token" do
      assert {:ok, token} = Invite.create_invite(3600, 20)
      assert {:ok, {_random, _expiry, 20, _signature}} = Invite.parse(token)
    end

    test "a minted token validates" do
      assert {:ok, token} = Invite.create_invite(3600, 5)
      assert :ok = Invite.validate_invite(token)
    end

    test "tokens are distinct" do
      assert {:ok, first} = Invite.create_invite(3600)
      assert {:ok, second} = Invite.create_invite(3600)
      refute first == second
    end
  end

  describe "validate_invite/1" do
    # The budget is only worth anything if it is signed. A holder who wants
    # more redemptions than they were given will try exactly this.
    test "raising the budget in a token invalidates its signature" do
      assert {:ok, token} = Invite.create_invite(3600, 2)
      assert {:ok, {random, expiry, 2, signature}} = Invite.parse(token)

      inflated = "nfs_inv_#{random}_#{expiry}_2000_#{signature}"

      assert {:error, :invalid_signature} = Invite.validate_invite(inflated)
    end

    test "a token with no budget field is not a token" do
      assert {:ok, token} = Invite.create_invite(3600)
      assert {:ok, {random, expiry, _uses, signature}} = Invite.parse(token)

      assert {:error, :invalid_format} =
               Invite.validate_invite("nfs_inv_#{random}_#{expiry}_#{signature}")
    end

    test "a zero or negative budget is rejected as malformed" do
      assert {:ok, token} = Invite.create_invite(3600)
      assert {:ok, {random, expiry, _uses, signature}} = Invite.parse(token)

      assert {:error, :invalid_format} =
               Invite.validate_invite("nfs_inv_#{random}_#{expiry}_0_#{signature}")

      assert {:error, :invalid_format} =
               Invite.validate_invite("nfs_inv_#{random}_#{expiry}_-1_#{signature}")
    end

    test "an expired token is rejected" do
      assert {:ok, token} = Invite.create_invite(1, 3)
      assert {:ok, {random, _expiry, uses, _signature}} = Invite.parse(token)

      past = DateTime.utc_now() |> DateTime.add(-10, :second) |> DateTime.to_unix()

      assert {:error, :expired} =
               Invite.validate_invite("nfs_inv_#{random}_#{past}_#{uses}_deadbeefdeadbeef")
    end

    test "a token signed by another cluster is rejected" do
      assert {:ok, token} = Invite.create_invite(3600, 4)
      assert {:ok, {random, expiry, uses, _signature}} = Invite.parse(token)

      assert {:error, :invalid_signature} =
               Invite.validate_invite("nfs_inv_#{random}_#{expiry}_#{uses}_aaaaaaaaaaaaaaaa")
    end
  end

  # The serving side reconstructs the token from the components a joining node
  # sends, and both the proof check and the response encryption key depend on
  # getting byte-identical output. A mismatch surfaces as a decryption failure
  # on the joining node, which names nothing.
  describe "signing_payload/3" do
    test "orders the components the way the token spells them" do
      assert Invite.signing_payload("rand", "1234567890", "7") == "rand_1234567890_7"
    end

    test "reconstructing a minted token from its parts reproduces it exactly" do
      assert {:ok, token} = Invite.create_invite(3600, 9)
      assert {:ok, {random, expiry, uses, signature}} = Invite.parse(token)
      assert {:ok, state} = State.load()

      expected =
        :crypto.mac(
          :hmac,
          :sha256,
          state.master_key,
          Invite.signing_payload(random, Integer.to_string(expiry), Integer.to_string(uses))
        )
        |> Base.encode32(case: :lower, padding: false)
        |> binary_part(0, 16)

      assert expected == signature
    end
  end
end
