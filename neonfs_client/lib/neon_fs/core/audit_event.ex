defmodule NeonFS.Core.AuditEvent do
  @moduledoc """
  Struct representing an audit log event.

  Captures security-relevant actions such as node lifecycle, volume operations,
  ACL changes, encryption operations, and authorisation denials.
  """

  @type event_type ::
          :node_joined
          | :node_left
          | :volume_created
          | :volume_deleted
          | :volume_acl_changed
          | :file_acl_changed
          | :encryption_enabled
          | :key_rotated
          | :rotation_completed
          | :authorisation_denied
          | :admin_action

  @type outcome :: :success | :denied

  @type t :: %__MODULE__{
          id: binary(),
          timestamp: DateTime.t(),
          event_type: event_type(),
          actor_uid: non_neg_integer(),
          actor_node: atom(),
          resource: binary() | nil,
          details: map(),
          outcome: outcome()
        }

  @enforce_keys [:event_type, :actor_uid]
  defstruct [
    :id,
    :timestamp,
    :event_type,
    :actor_uid,
    :actor_node,
    :resource,
    details: %{},
    outcome: :success
  ]

  @doc """
  Creates a new AuditEvent with auto-generated id and timestamp.
  """
  @spec new(keyword()) :: t()
  def new(attrs) do
    struct!(
      __MODULE__,
      Keyword.merge(
        [
          id: generate_id(),
          timestamp: DateTime.utc_now(),
          actor_node: Node.self()
        ],
        attrs
      )
    )
  end

  defp generate_id do
    Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
  end
end
