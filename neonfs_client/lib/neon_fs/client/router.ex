defmodule NeonFS.Client.Router do
  @moduledoc """
  Routes RPC calls to the best available core node.

  Uses `CostFunction` for node selection and includes retry + failover
  to the next-best node on `:badrpc`.
  """

  require Logger

  alias NeonFS.Client.{CostFunction, Discovery}

  @max_retries 2

  @doc """
  Routes a call to the cheapest available core node.

  Retries on `:badrpc` by failing over to the next available node.
  """
  @spec call(module(), atom(), [term()], keyword()) :: term()
  def call(module, function, args, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 10_000)
    do_call(module, function, args, timeout, @max_retries, [])
  end

  @doc """
  Routes a metadata call, preferring the Ra leader when possible.
  """
  @spec metadata_call(module(), atom(), [term()]) :: term()
  def metadata_call(module, function, args) do
    do_call(module, function, args, 10_000, @max_retries, prefer_leader: true)
  end

  ## Private helpers

  defp do_call(_module, _function, _args, _timeout, 0, _opts) do
    {:error, :all_nodes_unreachable}
  end

  defp do_call(module, function, args, timeout, retries_left, opts) do
    case select_node(opts) do
      {:ok, node} ->
        case :rpc.call(node, module, function, args, timeout) do
          {:badrpc, reason} ->
            Logger.warning(
              "RPC to #{node} failed: #{inspect(reason)}, retrying (#{retries_left - 1} left)"
            )

            do_call(module, function, args, timeout, retries_left - 1, opts)

          result ->
            result
        end

      {:error, :no_core_nodes} ->
        fallback_call(module, function, args, timeout)
    end
  end

  defp fallback_call(module, function, args, timeout) do
    case discover_any_core_node() do
      {:ok, node} -> fallback_rpc(node, module, function, args, timeout)
      :error -> {:error, :all_nodes_unreachable}
    end
  end

  defp fallback_rpc(node, module, function, args, timeout) do
    case :rpc.call(node, module, function, args, timeout) do
      {:badrpc, _} -> {:error, :all_nodes_unreachable}
      result -> result
    end
  end

  defp select_node(opts) do
    if Keyword.get(opts, :prefer_leader, false) do
      CostFunction.select_core_node(prefer_leader: true)
    else
      CostFunction.select_core_node()
    end
  end

  defp discover_any_core_node do
    case Discovery.get_core_nodes() do
      [node | _] -> {:ok, node}
      [] -> :error
    end
  end
end
