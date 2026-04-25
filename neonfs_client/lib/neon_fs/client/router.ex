defmodule NeonFS.Client.Router do
  @moduledoc """
  Routes RPC calls to the best available core node.

  Uses `CostFunction` for node selection and includes retry + failover
  to the next-best node on `:badrpc`.

  Also provides `data_call/4` for routing chunk data operations over
  the TLS data plane (Phase 9), using per-peer connection pools managed
  by `NeonFS.Transport.PoolManager`.
  """

  require Logger

  alias NeonFS.Client.{CostFunction, Discovery}
  alias NeonFS.Error.Unavailable
  alias NeonFS.Transport.{ConnPool, PoolManager}

  @max_retries 2
  @default_data_timeout 30_000

  @type data_operation :: :put_chunk | :get_chunk | :has_chunk

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
  Routes a chunk data operation to a specific node over the TLS data plane.

  Unlike `call/4` which uses CostFunction to select the best node,
  `data_call/4` targets an explicit node — chunk placement is determined
  by the metadata layer.

  ## Operations

    * `:put_chunk` — `args`: `[hash: h, volume_id: v, write_id: w, tier: t, data: d]`.
      The `:volume_id` arg historically carries the drive identifier (legacy
      naming). Pass `:processing_volume_id` alongside it to request that the
      receiving handler resolve the volume's compression / encryption
      settings before storing the chunk — the interface-side chunking path
      from the #408 design note.
    * `:get_chunk` — `args`: `[hash: h, volume_id: v]`
    * `:has_chunk` — `args`: `[hash: h]`

  ## Options

    * `:timeout` — checkout/recv timeout in ms (default: 30_000)

  ## Returns

    * `:ok` — put_chunk success
    * `{:ok, chunk_bytes}` — get_chunk success
    * `{:ok, %{tier: tier, size: size}}` — has_chunk found
    * `{:error, :no_data_endpoint}` — no pool exists for target node
    * `{:error, :ref_mismatch}` — response ref doesn't match request
    * `{:error, reason}` — remote error
  """
  @spec data_call(node(), data_operation(), keyword(), keyword()) ::
          :ok | {:ok, term()} | {:error, term()}
  def data_call(node, operation, args, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_data_timeout)

    case PoolManager.get_pool(node) do
      {:ok, pool} ->
        ref = make_ref()
        message = build_data_message(operation, ref, args)
        execute_opts = [timeout: timeout, recv_timeout: timeout]

        pool
        |> ConnPool.execute(message, execute_opts)
        |> normalise_data_response(ref)

      {:error, :no_pool} ->
        {:error, :no_data_endpoint}
    end
  end

  @doc """
  Routes a metadata call, preferring the Ra leader when possible.

  ## Options

    * `:timeout` — RPC timeout in ms (default: 10_000).
  """
  @spec metadata_call(module(), atom(), [term()], keyword()) :: term()
  def metadata_call(module, function, args, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 10_000)
    do_call(module, function, args, timeout, @max_retries, prefer_leader: true)
  end

  ## Private helpers — data_call

  defp build_data_message(:put_chunk, ref, args) do
    # `:volume_id` historically carries the drive identifier; callers that
    # want the receiving handler to apply volume-level compression /
    # encryption pass `:processing_volume_id` alongside it. Presence of
    # that key switches the frame to the new 8-tuple shape understood by
    # `NeonFS.Transport.Handler`.
    case args[:processing_volume_id] do
      nil ->
        {:put_chunk, ref, args[:hash], args[:volume_id], args[:write_id], args[:tier],
         args[:data]}

      volume_id when is_binary(volume_id) ->
        {:put_chunk, ref, args[:hash], volume_id, args[:volume_id], args[:write_id], args[:tier],
         args[:data]}
    end
  end

  defp build_data_message(:get_chunk, ref, args) do
    {:get_chunk, ref, args[:hash], args[:volume_id], Keyword.get(args, :tier, "hot")}
  end

  defp build_data_message(:has_chunk, ref, args) do
    {:has_chunk, ref, args[:hash]}
  end

  defp normalise_data_response({:ok, ref}, expected_ref) when ref == expected_ref, do: :ok

  defp normalise_data_response({:ok, ref, data}, expected_ref) when ref == expected_ref,
    do: {:ok, data}

  defp normalise_data_response({:ok, ref, tier, size}, expected_ref) when ref == expected_ref,
    do: {:ok, %{tier: tier, size: size}}

  defp normalise_data_response({:error, ref, reason}, expected_ref) when ref == expected_ref,
    do: {:error, reason}

  defp normalise_data_response({:ok, _wrong_ref}, _expected_ref), do: {:error, :ref_mismatch}
  defp normalise_data_response({:ok, _wrong_ref, _}, _expected_ref), do: {:error, :ref_mismatch}

  defp normalise_data_response({:ok, _wrong_ref, _, _}, _expected_ref),
    do: {:error, :ref_mismatch}

  defp normalise_data_response({:error, _wrong_ref, _}, _expected_ref),
    do: {:error, :ref_mismatch}

  ## Private helpers — call/metadata_call

  defp do_call(_module, _function, _args, _timeout, 0, _opts) do
    {:error, Unavailable.exception(message: "All core nodes unreachable")}
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
      :error -> {:error, Unavailable.exception(message: "All core nodes unreachable")}
    end
  end

  defp fallback_rpc(node, module, function, args, timeout) do
    case :rpc.call(node, module, function, args, timeout) do
      {:badrpc, reason} ->
        {:error,
         Unavailable.exception(
           message: "RPC to #{node} failed: #{inspect(reason)}",
           details: %{node: node, reason: reason}
         )}

      result ->
        result
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
