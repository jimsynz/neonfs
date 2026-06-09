defmodule NeonFS.Error.Conflict do
  @moduledoc """
  A resource is transiently contended — another holder owns a lock,
  namespace claim, or write intent on it right now.

  Distinct from `NeonFS.Error.AlreadyExists` (a permanent collision):
  a conflict may clear once the holder releases. Interface layers map it
  to `EAGAIN`/`EBUSY` (FUSE), an NLM `denied` reply (NFS locking), or
  HTTP 409.

  `conflicting` carries the detail of whoever holds the resource — a
  lock-holder map (`%{type:, range:, svid:, oh:}`), a namespace claim id,
  or the conflicting `Intent`. `reason` preserves the originating tag
  (`:conflict`, `:busy`).
  """
  use Splode.Error, fields: [:conflicting, reason: :conflict], class: :conflict

  @type t :: %__MODULE__{}

  @doc """
  Builds a `Conflict` error, preserving the originating tag under `reason`
  and the holder/claim/intent detail under `conflicting`.
  """
  @spec from_reason(atom(), term()) :: t()
  def from_reason(reason, conflicting \\ nil) when is_atom(reason) do
    exception(reason: reason, conflicting: conflicting)
  end

  @impl true
  def message(%{reason: :busy}), do: "Resource busy"
  def message(_), do: "Resource conflict"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
