defmodule NeonFS.CLI.Handler.Common do
  @moduledoc """
  Cross-cutting helpers shared by the CLI command-group handlers.

  `NeonFS.CLI.Handler` and the per-group modules it delegates to
  (`NeonFS.CLI.Handler.S3`, …) `import` this module so the common
  request preamble (`set_cli_metadata/0`, `require_cluster/0`) and the
  error-normalisation (`wrap_error/1`) live in one place rather than
  being copied into every group. Extracted while splitting the handler
  per #1203.
  """

  alias NeonFS.Cluster.State
  alias NeonFS.Error.{Internal, NotFound}

  @doc """
  Returns `:ok` when a cluster has been initialised, or a `NotFound`
  error instructing the operator to init/join first.
  """
  @spec require_cluster() :: :ok | {:error, NotFound.t()}
  def require_cluster do
    if State.exists?() do
      :ok
    else
      {:error,
       NotFound.exception(
         message:
           "Cluster not initialised. Run 'neonfs cluster init' or 'neonfs cluster join' first."
       )}
    end
  end

  @doc """
  Tags the calling process's logger metadata with the `:cli` component
  and a fresh request id, so every command's logs are correlatable.
  """
  @spec set_cli_metadata() :: :ok
  def set_cli_metadata do
    Logger.metadata(
      component: :cli,
      request_id: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    )
  end

  @doc """
  Wraps a legacy error reason into a structured `NeonFS.Error`.
  Already-structured errors (Splode exceptions with a class field) pass
  through unchanged.
  """
  @spec wrap_error(term()) :: Exception.t()
  def wrap_error(%{__exception__: true, class: _} = error), do: error

  def wrap_error(reason) when is_binary(reason),
    do: Internal.exception(message: reason)

  def wrap_error(reason) when is_atom(reason),
    do: Internal.exception(message: Atom.to_string(reason))

  def wrap_error(reason),
    do: Internal.exception(message: inspect(reason))
end
