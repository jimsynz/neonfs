defmodule NeonFS.Core.IAMTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.IAM
  alias NeonFS.Core.IAM.User

  setup do
    start_iam_manager()
    :ok
  end

  describe "register/1" do
    test "creates a user, hashes the password, returns the public projection" do
      assert {:ok, user} =
               IAM.register(%{email: "Alice@example.com", password: "correct horse battery"})

      assert user.email == "alice@example.com"
      assert user.role == :user
      assert user.disabled? == false
      refute Map.has_key?(user, :password_hash)
    end

    test "honours an explicit role" do
      assert {:ok, %{role: :admin}} =
               IAM.register(%{
                 email: "boss@example.com",
                 password: "correct horse battery",
                 role: :admin
               })
    end

    test "rejects a duplicate email (case-insensitive)" do
      :ok = register_ok("dup@example.com")

      assert {:error, :email_taken} =
               IAM.register(%{email: "DUP@example.com", password: "correct horse battery"})
    end

    test "rejects a missing email" do
      assert {:error, :email_required} = IAM.register(%{password: "correct horse battery"})

      assert {:error, :email_required} =
               IAM.register(%{email: "", password: "correct horse battery"})
    end

    test "rejects an obviously invalid email" do
      assert {:error, :email_invalid} =
               IAM.register(%{email: "no-at-sign", password: "correct horse battery"})
    end

    test "rejects a missing or short password" do
      assert {:error, :password_required} = IAM.register(%{email: "x@example.com"})

      assert {:error, :password_too_short} =
               IAM.register(%{email: "x@example.com", password: "short"})
    end
  end

  describe "authenticate/2" do
    setup do
      :ok = register_ok("auth@example.com", "correct horse battery", role: :admin)
      :ok
    end

    test "succeeds with the correct password" do
      assert {:ok, user} = IAM.authenticate("auth@example.com", "correct horse battery")
      assert user.email == "auth@example.com"
      assert user.role == :admin
    end

    test "is case-insensitive on email" do
      assert {:ok, _} = IAM.authenticate("AUTH@example.com", "correct horse battery")
    end

    test "rejects a wrong password" do
      assert {:error, :invalid_credentials} =
               IAM.authenticate("auth@example.com", "wrong horse battery")
    end

    test "rejects an unknown email without leaking which" do
      assert {:error, :invalid_credentials} =
               IAM.authenticate("nobody@example.com", "correct horse battery")
    end

    test "rejects a disabled account" do
      {:ok, user} = IAM.get_by_email("auth@example.com")
      {:ok, _} = IAM.set_disabled(user.id, true)

      assert {:error, :disabled} =
               IAM.authenticate("auth@example.com", "correct horse battery")
    end
  end

  describe "set_role/2 and set_disabled/2" do
    test "round-trips through ETS / Ra" do
      {:ok, user} = IAM.register(%{email: "role@example.com", password: "correct horse battery"})

      assert {:ok, %{role: :admin}} = IAM.set_role(user.id, :admin)
      assert {:ok, public} = IAM.get(user.id)
      assert public.role == :admin

      assert {:ok, %{disabled?: true}} = IAM.set_disabled(user.id, true)
      assert {:ok, %{disabled?: true}} = IAM.get(user.id)
    end

    test "returns :not_found for unknown ids" do
      assert {:error, :not_found} = IAM.set_role("missing", :admin)
      assert {:error, :not_found} = IAM.set_disabled("missing", true)
    end
  end

  describe "change_password/2" do
    test "old password no longer authenticates after change" do
      {:ok, user} =
        IAM.register(%{email: "pw@example.com", password: "first horse battery"})

      assert {:ok, _} = IAM.change_password(user.id, "second horse battery")

      assert {:error, :invalid_credentials} =
               IAM.authenticate("pw@example.com", "first horse battery")

      assert {:ok, _} = IAM.authenticate("pw@example.com", "second horse battery")
    end

    test "rejects a too-short replacement password" do
      {:ok, user} =
        IAM.register(%{email: "pw2@example.com", password: "first horse battery"})

      assert {:error, :password_too_short} = IAM.change_password(user.id, "x")
    end
  end

  describe "delete/1" do
    test "removes the user" do
      {:ok, user} = IAM.register(%{email: "gone@example.com", password: "correct horse battery"})
      assert :ok = IAM.delete(user.id)
      assert {:error, :not_found} = IAM.get(user.id)
    end
  end

  describe "list/0" do
    test "returns the public projection for every user" do
      :ok = register_ok("a@example.com")
      :ok = register_ok("b@example.com")

      emails = IAM.list() |> Enum.map(& &1.email) |> Enum.sort()
      assert emails == ["a@example.com", "b@example.com"]

      Enum.each(IAM.list(), fn u -> refute Map.has_key?(u, :password_hash) end)
    end
  end

  describe "User struct" do
    test "Inspect redacts the password hash" do
      user = %User{
        id: "abc",
        email: "x@example.com",
        password_hash: "secret-bcrypt-hash",
        role: :user,
        disabled?: false,
        created_at: ~U[2026-01-01 00:00:00Z],
        updated_at: ~U[2026-01-01 00:00:00Z]
      }

      rendered = inspect(user)
      refute rendered =~ "secret-bcrypt-hash"
      refute rendered =~ "password_hash"
    end
  end

  defp register_ok(email, password \\ "correct horse battery", opts \\ []) do
    role = Keyword.get(opts, :role, :user)
    {:ok, _} = IAM.register(%{email: email, password: password, role: role})
    :ok
  end
end
