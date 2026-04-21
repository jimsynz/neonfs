defmodule NeonFS.Core.IAM do
  @moduledoc """
  Public API for the NeonFS identity and access management subsystem.

  Doubles as the placeholder Ash domain for future
  resource-modelled IAM types (groups, access policies, identity
  mappings — #290 / #291 / #292). The first user-facing concept,
  `NeonFS.Core.IAM.User`, is currently a plain struct because the
  IAM data path is Ra-backed (no Ecto data layer) and the available
  password strategies in `ash_authentication` assume one — see the
  module doc on `NeonFS.Core.IAM.User` for the rationale.

  ## Functions

  * `register/1` — create a new user with `email` + `password`.
    Hashes the password with bcrypt and serialises the write
    through `IAMManager` (which enforces email uniqueness).
  * `authenticate/2` — match an `email` + `password` pair against
    a registered user. Constant-time on the password verify.
  * `get/1`, `get_by_email/1`, `list/0` — direct ETS reads.
  * `set_role/2`, `set_disabled/2`, `change_password/2`,
    `delete/1` — Ra-replicated mutations.

  No public function ever returns the `password_hash` field — every
  reply is run through `User.public/1`.
  """

  use Ash.Domain

  alias NeonFS.Core.IAM.{Manager, User}

  resources do
  end

  @typedoc "Reasons `register/1` may refuse with."
  @type register_error ::
          :email_required
          | :email_invalid
          | :email_taken
          | :password_required
          | :password_too_short

  @typedoc "Reasons `authenticate/2` may refuse with."
  @type authenticate_error ::
          :invalid_credentials | :disabled

  @password_min_length 12

  @doc """
  Create a new user. Required attrs: `:email`, `:password`.
  Optional: `:role` (`:user | :admin`, default `:user`).

  Returns the public projection of the user (no password hash) on
  success.
  """
  @spec register(map()) :: {:ok, map()} | {:error, register_error()}
  def register(attrs) when is_map(attrs) do
    with {:ok, email} <- validate_email(Map.get(attrs, :email)),
         {:ok, password} <- validate_password(Map.get(attrs, :password)) do
      role = parse_role(Map.get(attrs, :role, :user))
      now = DateTime.utc_now()

      user = %User{
        id: UUIDv7.generate(),
        email: email,
        password_hash: Bcrypt.hash_pwd_salt(password),
        role: role,
        disabled?: false,
        created_at: now,
        updated_at: now
      }

      case Manager.put_user(user) do
        :ok -> {:ok, User.public(user)}
        {:error, :email_taken} -> {:error, :email_taken}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @doc """
  Verify `email` + `password` against a registered user. Constant-time
  on the password verify so timing doesn't reveal whether the email
  exists.
  """
  @spec authenticate(String.t(), String.t()) ::
          {:ok, map()} | {:error, authenticate_error()}
  def authenticate(email, password) when is_binary(email) and is_binary(password) do
    case get_by_email(email) do
      {:ok, %User{} = user} -> verify_password(user, password)
      {:error, :not_found} -> reject_unknown_email(password)
    end
  end

  @doc """
  Fetch a user by id. Returns the public projection.
  """
  @spec get(binary()) :: {:ok, map()} | {:error, :not_found}
  def get(id) when is_binary(id) do
    case fetch_user(id) do
      {:ok, user} -> {:ok, User.public(user)}
      {:error, :not_found} -> {:error, :not_found}
    end
  end

  @doc """
  Fetch a user by case-insensitive email. Returns the public projection.
  """
  @spec get_by_email(String.t()) :: {:ok, User.t()} | {:error, :not_found}
  def get_by_email(email) when is_binary(email) do
    needle = User.normalise_email(email)

    :iam_users
    |> Manager.list()
    |> Enum.find_value({:error, :not_found}, fn {_id, record} ->
      if Map.get(record, :email) == needle, do: {:ok, struct(User, record)}, else: nil
    end)
  end

  @doc "List every user as the public projection."
  @spec list() :: [map()]
  def list do
    :iam_users
    |> Manager.list()
    |> Enum.map(fn {_id, record} -> record |> then(&struct(User, &1)) |> User.public() end)
  end

  @doc "Set the role for an existing user."
  @spec set_role(binary(), User.role()) :: {:ok, map()} | {:error, :not_found}
  def set_role(id, role) when is_binary(id) and role in [:user, :admin] do
    update_user(id, %{role: role})
  end

  @doc "Toggle the `disabled?` flag for an existing user."
  @spec set_disabled(binary(), boolean()) :: {:ok, map()} | {:error, :not_found}
  def set_disabled(id, disabled?) when is_binary(id) and is_boolean(disabled?) do
    update_user(id, %{disabled?: disabled?})
  end

  @doc """
  Replace the user's password with a freshly-bcrypted hash. The
  caller is responsible for verifying the old password before
  invoking this — `change_password/2` does not re-prompt.
  """
  @spec change_password(binary(), String.t()) ::
          {:ok, map()} | {:error, :not_found | :password_too_short}
  def change_password(id, new_password) when is_binary(id) and is_binary(new_password) do
    with {:ok, password} <- validate_password(new_password) do
      update_user(id, %{password_hash: Bcrypt.hash_pwd_salt(password)})
    end
  end

  @doc "Permanently delete a user."
  @spec delete(binary()) :: :ok
  def delete(id) when is_binary(id) do
    Manager.delete(:iam_users, id)
  end

  # ——— Internal ————————————————————————————————————————————————

  defp fetch_user(id) do
    case Manager.get(:iam_users, id) do
      {:ok, record} -> {:ok, struct(User, record)}
      {:error, :not_found} -> {:error, :not_found}
    end
  end

  defp update_user(id, changes) do
    case fetch_user(id) do
      {:ok, user} ->
        updated = struct(user, Map.put(changes, :updated_at, DateTime.utc_now()))

        case Manager.put_user(updated, user.id) do
          :ok -> {:ok, User.public(updated)}
          {:error, reason} -> {:error, reason}
        end

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  defp verify_password(%User{disabled?: true}, _password), do: {:error, :disabled}

  defp verify_password(%User{password_hash: hash} = user, password) do
    if Bcrypt.verify_pass(password, hash) do
      {:ok, User.public(user)}
    else
      {:error, :invalid_credentials}
    end
  end

  # Run a no-op bcrypt verify so unknown-email replies take the same
  # wall-clock time as wrong-password replies.
  defp reject_unknown_email(password) do
    Bcrypt.no_user_verify(password: password)
    {:error, :invalid_credentials}
  end

  defp validate_email(nil), do: {:error, :email_required}
  defp validate_email(""), do: {:error, :email_required}

  defp validate_email(email) when is_binary(email) do
    normalised = User.normalise_email(email)

    if String.contains?(normalised, "@") and String.length(normalised) <= 254 do
      {:ok, normalised}
    else
      {:error, :email_invalid}
    end
  end

  defp validate_email(_), do: {:error, :email_invalid}

  defp validate_password(nil), do: {:error, :password_required}
  defp validate_password(""), do: {:error, :password_required}

  defp validate_password(password) when is_binary(password) do
    if String.length(password) >= @password_min_length do
      {:ok, password}
    else
      {:error, :password_too_short}
    end
  end

  defp validate_password(_), do: {:error, :password_required}

  defp parse_role(:admin), do: :admin
  defp parse_role(_), do: :user
end
