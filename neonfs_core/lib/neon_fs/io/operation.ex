defmodule NeonFS.IO.Operation do
  @moduledoc """
  An I/O operation submitted to the scheduler.

  Each operation carries a priority class, a target drive, a volume ID
  (for weighted fair queuing), and a zero-arity callback that performs
  the actual I/O work. The scheduler controls *when* and *where* the
  callback executes, not *what* it does.
  """

  alias NeonFS.IO.Priority

  @type t :: %__MODULE__{
          id: String.t(),
          priority: Priority.t(),
          volume_id: String.t(),
          drive_id: String.t(),
          type: :read | :write,
          callback: (-> term()),
          submitted_at: integer(),
          metadata: map()
        }

  @enforce_keys [:priority, :volume_id, :drive_id, :type, :callback]
  defstruct [
    :callback,
    :drive_id,
    :id,
    :priority,
    :type,
    :volume_id,
    metadata: %{},
    submitted_at: 0
  ]

  @doc """
  Creates a new operation with a generated ID and timestamp.

  ## Options

  All fields from the struct are accepted as keyword options.
  Required: `:priority`, `:volume_id`, `:drive_id`, `:type`, `:callback`.

  ## Examples

      iex> op = NeonFS.IO.Operation.new(
      ...>   priority: :user_read,
      ...>   volume_id: "vol-1",
      ...>   drive_id: "nvme0",
      ...>   type: :read,
      ...>   callback: fn -> :ok end
      ...> )
      iex> is_binary(op.id)
      true
  """
  @spec new(keyword()) :: t()
  def new(attrs) when is_list(attrs) do
    attrs
    |> Keyword.put_new_lazy(:id, &generate_id/0)
    |> Keyword.put_new(:submitted_at, System.monotonic_time(:millisecond))
    |> then(&struct!(__MODULE__, &1))
  end

  @doc """
  Validates an operation, returning `{:ok, operation}` or `{:error, reason}`.

  Checks that all required fields are present and have valid values.
  """
  @spec validate(t()) :: {:ok, t()} | {:error, String.t()}
  def validate(%__MODULE__{} = op) do
    with :ok <- validate_id(op.id),
         :ok <- validate_priority(op.priority),
         :ok <- validate_volume_id(op.volume_id),
         :ok <- validate_drive_id(op.drive_id),
         :ok <- validate_type(op.type),
         :ok <- validate_callback(op.callback) do
      {:ok, op}
    end
  end

  defp generate_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end

  defp validate_callback(cb) when is_function(cb, 0), do: :ok
  defp validate_callback(_), do: {:error, "callback must be a zero-arity function"}

  defp validate_drive_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_drive_id(_), do: {:error, "drive_id must be a non-empty string"}

  defp validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(_), do: {:error, "id must be a non-empty string"}

  defp validate_priority(priority) do
    if Priority.valid?(priority),
      do: :ok,
      else: {:error, "invalid priority: #{inspect(priority)}"}
  end

  defp validate_type(type) when type in [:read, :write], do: :ok
  defp validate_type(type), do: {:error, "type must be :read or :write, got: #{inspect(type)}"}

  defp validate_volume_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_volume_id(_), do: {:error, "volume_id must be a non-empty string"}
end
