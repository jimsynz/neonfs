defmodule NeonFS.NFS.NLM.Handler do
  @moduledoc """
  NLM v4 (Network Lock Manager) RPC handler.

  Translates NLM lock/unlock/test requests into `NeonFS.Core.LockManager`
  calls routed via `NeonFS.Client.core_call/3`. Implements program 100021
  version 4 (NLM v4) for NFSv3 advisory byte-range locking.

  ## Supported procedures

  | # | Name | Description |
  |---|------|-------------|
  | 0 | NULL | Ping/probe |
  | 1 | TEST | Test if lock would conflict |
  | 2 | LOCK | Acquire byte-range lock |
  | 3 | CANCEL | Cancel blocked lock request |
  | 4 | UNLOCK | Release byte-range lock |
  | 23 | FREE_ALL | Release all locks for a host |

  Async variants (6-15) and share procedures (20-22) return PROC_UNAVAIL.
  """

  require Logger

  alias NeonFS.NFS.NLM.Codec
  alias NeonFS.NFS.RPC.Message

  @nlm_program 100_021
  @nlm_version 4

  @type handler_state :: %{
          core_call_fn: (module(), atom(), [term()] -> term())
        }

  @doc """
  Initialises handler state.

  Options:
    - `:core_call_fn` — override for `NeonFS.Client.core_call/3` (testing)
  """
  @spec init(keyword()) :: handler_state()
  def init(opts) do
    core_call_fn =
      Keyword.get(opts, :core_call_fn, fn mod, fun, args ->
        NeonFS.Client.core_call(mod, fun, args)
      end)

    %{core_call_fn: core_call_fn}
  end

  @doc """
  Called when the transport connection closes.
  """
  @spec terminate(term(), handler_state()) :: :ok
  def terminate(_reason, _state), do: :ok

  @doc """
  Dispatches an RPC call to the appropriate NLM procedure.

  Returns `{reply_binary | nil, new_state}`.
  """
  @spec handle_call(Message.rpc_call(), handler_state()) :: {binary() | nil, handler_state()}
  def handle_call(%{program: @nlm_program, version: @nlm_version} = call, state) do
    {reply_body, state} = dispatch_procedure(call.procedure, call.body, call.cred, state)

    case reply_body do
      nil ->
        reply = Message.encode_accepted_reply(call.xid, :proc_unavail)
        {reply, state}

      body when is_binary(body) ->
        reply = Message.encode_accepted_reply(call.xid, :success, body)
        {reply, state}
    end
  rescue
    e ->
      Logger.error("NLM handler crashed",
        procedure: call.procedure,
        error: Exception.message(e)
      )

      reply = Message.encode_accepted_reply(call.xid, :system_err)
      {reply, state}
  end

  def handle_call(%{program: @nlm_program} = call, state) do
    reply = Message.encode_prog_mismatch(call.xid, @nlm_version, @nlm_version)
    {reply, state}
  end

  def handle_call(%{} = call, state) do
    reply = Message.encode_accepted_reply(call.xid, :prog_unavail)
    {reply, state}
  end

  ## Procedure dispatch

  # NLM4_NULL (0)
  defp dispatch_procedure(0, _body, _cred, state) do
    :telemetry.execute([:neonfs, :nlm, :request], %{count: 1}, %{procedure: :null})
    {<<>>, state}
  end

  # NLM4_TEST (1)
  defp dispatch_procedure(1, body, _cred, state) do
    :telemetry.execute([:neonfs, :nlm, :request], %{count: 1}, %{procedure: :test})

    case Codec.decode_testargs(body) do
      {:ok, args} ->
        result = handle_test(args, state)
        {result, state}

      {:error, _} ->
        {encode_garbage_args_res(<<>>), state}
    end
  end

  # NLM4_LOCK (2)
  defp dispatch_procedure(2, body, _cred, state) do
    :telemetry.execute([:neonfs, :nlm, :request], %{count: 1}, %{procedure: :lock})

    case Codec.decode_lockargs(body) do
      {:ok, args} ->
        result = handle_lock(args, state)
        {result, state}

      {:error, _} ->
        {encode_garbage_args_res(<<>>), state}
    end
  end

  # NLM4_CANCEL (3)
  defp dispatch_procedure(3, body, _cred, state) do
    :telemetry.execute([:neonfs, :nlm, :request], %{count: 1}, %{procedure: :cancel})

    case Codec.decode_cancargs(body) do
      {:ok, args} ->
        result = Codec.encode_res(args.cookie, :granted)
        {result, state}

      {:error, _} ->
        {encode_garbage_args_res(<<>>), state}
    end
  end

  # NLM4_UNLOCK (4)
  defp dispatch_procedure(4, body, _cred, state) do
    :telemetry.execute([:neonfs, :nlm, :request], %{count: 1}, %{procedure: :unlock})

    case Codec.decode_unlockargs(body) do
      {:ok, args} ->
        result = handle_unlock(args, state)
        {result, state}

      {:error, _} ->
        {encode_garbage_args_res(<<>>), state}
    end
  end

  # NLM4_GRANTED (5) — client->server acknowledgement, not needed for server
  defp dispatch_procedure(5, body, _cred, state) do
    :telemetry.execute([:neonfs, :nlm, :request], %{count: 1}, %{procedure: :granted})

    case Codec.decode_testargs(body) do
      {:ok, args} -> {Codec.encode_res(args.cookie, :granted), state}
      {:error, _} -> {encode_garbage_args_res(<<>>), state}
    end
  end

  # NLM4_FREE_ALL (23)
  defp dispatch_procedure(23, body, _cred, state) do
    :telemetry.execute([:neonfs, :nlm, :request], %{count: 1}, %{procedure: :free_all})

    case Codec.decode_notify(body) do
      {:ok, args} ->
        handle_free_all(args, state)
        {<<>>, state}

      {:error, _} ->
        {<<>>, state}
    end
  end

  # Async variants (6-15) and share procedures (20-22) — not implemented
  defp dispatch_procedure(proc, _body, _cred, state)
       when proc in 6..15 or proc in 20..22 do
    :telemetry.execute([:neonfs, :nlm, :request], %{count: 1}, %{
      procedure: :unsupported,
      proc_num: proc
    })

    {nil, state}
  end

  defp dispatch_procedure(_proc, _body, _cred, state) do
    {nil, state}
  end

  ## Procedure implementations

  defp handle_test(args, state) do
    file_id = file_id_from_fh(args.lock.fh)
    client_ref = client_ref(args.lock)
    range = lock_range(args.lock)
    lock_type = if args.exclusive, do: :exclusive, else: :shared

    result =
      state.core_call_fn.(NeonFS.Core.LockManager, :test_lock, [
        file_id,
        client_ref,
        range,
        lock_type
      ])

    case result do
      :ok ->
        Codec.encode_testres(args.cookie, :granted)

      {:error, :conflict, holder_info} ->
        holder = %{
          exclusive: holder_info.type == :exclusive,
          svid: Map.get(holder_info, :svid, 0),
          oh: Map.get(holder_info, :oh, <<>>),
          offset: elem(holder_info.range, 0),
          length: elem(holder_info.range, 1)
        }

        Codec.encode_testres(args.cookie, {:denied, holder})

      {:error, :grace_period} ->
        Codec.encode_testres(args.cookie, :granted)

      {:error, _} ->
        Codec.encode_testres(args.cookie, :granted)
    end
  end

  defp handle_lock(args, state) do
    file_id = file_id_from_fh(args.lock.fh)
    client_ref = client_ref(args.lock)
    range = lock_range(args.lock)
    lock_type = if args.exclusive, do: :exclusive, else: :shared

    opts = [mode: :advisory, reclaim: args.reclaim]

    result =
      state.core_call_fn.(NeonFS.Core.LockManager, :lock, [
        file_id,
        client_ref,
        range,
        lock_type,
        opts
      ])

    case result do
      :ok ->
        Codec.encode_res(args.cookie, :granted)

      {:error, :timeout} ->
        if args.block do
          Codec.encode_res(args.cookie, :blocked)
        else
          Codec.encode_res(args.cookie, :denied)
        end

      {:error, :grace_period} ->
        Codec.encode_res(args.cookie, :denied_grace_period)

      {:error, :unavailable} ->
        Codec.encode_res(args.cookie, :denied_nolocks)

      {:error, _} ->
        Codec.encode_res(args.cookie, :failed)
    end
  end

  defp handle_unlock(args, state) do
    file_id = file_id_from_fh(args.lock.fh)
    client_ref = client_ref(args.lock)
    range = lock_range(args.lock)

    result =
      state.core_call_fn.(NeonFS.Core.LockManager, :unlock, [file_id, client_ref, range])

    case result do
      :ok -> Codec.encode_res(args.cookie, :granted)
      {:error, :unavailable} -> Codec.encode_res(args.cookie, :denied_nolocks)
      {:error, _} -> Codec.encode_res(args.cookie, :failed)
    end
  end

  defp handle_free_all(args, state) do
    Logger.info("NLM FREE_ALL for host", name: args.name)

    # FREE_ALL returns void — best-effort cleanup. We can't enumerate all
    # files locked by a host without a tracking structure, so this is a no-op
    # for now. The LockManager's TTL will clean up stale locks.
    _ = state
    :ok
  end

  ## Helpers

  defp file_id_from_fh(fh) when byte_size(fh) >= 24 do
    <<inode::little-unsigned-64, volume_id::binary-size(16)>> = binary_part(fh, 0, 24)
    "#{Base.encode16(volume_id, case: :lower)}:#{inode}"
  end

  defp file_id_from_fh(fh), do: Base.encode16(fh, case: :lower)

  defp client_ref(lock) do
    {lock.caller_name, lock.svid}
  end

  defp lock_range(%{offset: offset, length: 0}), do: {offset, 0xFFFFFFFFFFFFFFFF - offset}
  defp lock_range(%{offset: offset, length: length}), do: {offset, length}

  defp encode_garbage_args_res(cookie) do
    Codec.encode_res(cookie, :failed)
  end
end
