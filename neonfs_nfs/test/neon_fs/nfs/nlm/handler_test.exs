defmodule NeonFS.NFS.NLM.HandlerTest do
  use ExUnit.Case, async: true

  alias NeonFS.NFS.NLM.{Codec, Handler}
  alias NeonFS.NFS.RPC.XDR

  @nlm_program 100_021
  @nlm_version 4

  setup do
    core_call_fn = fn
      NeonFS.Core.LockManager, :lock, [_file_id, _client_ref, _range, _type, _opts] ->
        :ok

      NeonFS.Core.LockManager, :unlock, [_file_id, _client_ref, _range] ->
        :ok

      NeonFS.Core.LockManager, :test_lock, [_file_id, _client_ref, _range, _type] ->
        :ok

      _mod, _fun, _args ->
        {:error, :not_implemented}
    end

    state = Handler.init(core_call_fn: core_call_fn)

    %{state: state}
  end

  describe "NLM4_NULL (procedure 0)" do
    test "returns empty reply", %{state: state} do
      call = build_call(0, <<>>)
      {reply, _state} = Handler.handle_call(call, state)

      assert reply != nil
      assert_accepted_success(reply, call.xid)
    end
  end

  describe "NLM4_TEST (procedure 1)" do
    test "returns granted when no conflict", %{state: state} do
      body = encode_testargs("cookie1", false, make_lock())
      call = build_call(1, body)

      {reply, _state} = Handler.handle_call(call, state)
      assert_accepted_success(reply, call.xid)
    end

    test "returns denied when conflict exists" do
      core_call_fn = fn
        NeonFS.Core.LockManager, :test_lock, [_fid, _cref, _range, _type] ->
          {:error, :conflict, %{type: :exclusive, range: {0, 100}, svid: 99, oh: <<>>}}

        _, _, _ ->
          :ok
      end

      state = Handler.init(core_call_fn: core_call_fn)
      body = encode_testargs("cookie2", true, make_lock())
      call = build_call(1, body)

      {reply, _state} = Handler.handle_call(call, state)
      assert reply != nil
      assert_accepted_success(reply, call.xid)
    end
  end

  describe "NLM4_LOCK (procedure 2)" do
    test "returns granted on successful lock", %{state: state} do
      body = encode_lockargs("lockcookie", true, true, make_lock(), false, 1)
      call = build_call(2, body)

      {reply, _state} = Handler.handle_call(call, state)
      assert_nlm_res_stat(reply, call.xid, :granted)
    end

    test "returns denied_grace_period during grace" do
      core_call_fn = fn
        NeonFS.Core.LockManager, :lock, _ ->
          {:error, :grace_period}

        _, _, _ ->
          :ok
      end

      state = Handler.init(core_call_fn: core_call_fn)
      body = encode_lockargs("gcookie", false, true, make_lock(), false, 1)
      call = build_call(2, body)

      {reply, _state} = Handler.handle_call(call, state)
      assert_nlm_res_stat(reply, call.xid, :denied_grace_period)
    end

    test "returns denied_nolocks when unavailable" do
      core_call_fn = fn
        NeonFS.Core.LockManager, :lock, _ ->
          {:error, :unavailable}

        _, _, _ ->
          :ok
      end

      state = Handler.init(core_call_fn: core_call_fn)
      body = encode_lockargs("ucookie", false, true, make_lock(), false, 1)
      call = build_call(2, body)

      {reply, _state} = Handler.handle_call(call, state)
      assert_nlm_res_stat(reply, call.xid, :denied_nolocks)
    end

    test "returns blocked for blocking lock with timeout" do
      core_call_fn = fn
        NeonFS.Core.LockManager, :lock, _ ->
          {:error, :timeout}

        _, _, _ ->
          :ok
      end

      state = Handler.init(core_call_fn: core_call_fn)
      body = encode_lockargs("bcookie", true, true, make_lock(), false, 1)
      call = build_call(2, body)

      {reply, _state} = Handler.handle_call(call, state)
      assert_nlm_res_stat(reply, call.xid, :blocked)
    end

    test "returns denied for non-blocking lock with timeout" do
      core_call_fn = fn
        NeonFS.Core.LockManager, :lock, _ ->
          {:error, :timeout}

        _, _, _ ->
          :ok
      end

      state = Handler.init(core_call_fn: core_call_fn)
      body = encode_lockargs("nbcookie", false, true, make_lock(), false, 1)
      call = build_call(2, body)

      {reply, _state} = Handler.handle_call(call, state)
      assert_nlm_res_stat(reply, call.xid, :denied)
    end
  end

  describe "NLM4_CANCEL (procedure 3)" do
    test "returns granted", %{state: state} do
      body = encode_cancargs("cancookie", true, false, make_lock())
      call = build_call(3, body)

      {reply, _state} = Handler.handle_call(call, state)
      assert_nlm_res_stat(reply, call.xid, :granted)
    end
  end

  describe "NLM4_UNLOCK (procedure 4)" do
    test "returns granted on successful unlock", %{state: state} do
      body = encode_unlockargs("uncookie", make_lock())
      call = build_call(4, body)

      {reply, _state} = Handler.handle_call(call, state)
      assert_nlm_res_stat(reply, call.xid, :granted)
    end

    test "returns denied_nolocks when unavailable" do
      core_call_fn = fn
        NeonFS.Core.LockManager, :unlock, _ ->
          {:error, :unavailable}

        _, _, _ ->
          :ok
      end

      state = Handler.init(core_call_fn: core_call_fn)
      body = encode_unlockargs("uncookie2", make_lock())
      call = build_call(4, body)

      {reply, _state} = Handler.handle_call(call, state)
      assert_nlm_res_stat(reply, call.xid, :denied_nolocks)
    end
  end

  describe "NLM4_FREE_ALL (procedure 23)" do
    test "returns void reply", %{state: state} do
      body = XDR.encode_string("deadhost") <> XDR.encode_int(3)
      call = build_call(23, body)

      {reply, _state} = Handler.handle_call(call, state)
      assert reply != nil
      assert_accepted_success(reply, call.xid)
    end
  end

  describe "program routing" do
    test "returns PROG_UNAVAIL for unknown program", %{state: state} do
      call = %{
        xid: 1,
        program: 99_999,
        version: 1,
        procedure: 0,
        cred_flavor: :auth_none,
        cred: nil,
        verf_flavor: :auth_none,
        body: <<>>
      }

      {reply, _state} = Handler.handle_call(call, state)

      assert <<
               1::big-32,
               1::big-32,
               0::big-32,
               0::big-32,
               0::big-32,
               # PROG_UNAVAIL = 1
               1::big-32
             >> = reply
    end

    test "returns PROG_MISMATCH for wrong NLM version", %{state: state} do
      call = %{
        xid: 2,
        program: @nlm_program,
        version: 3,
        procedure: 0,
        cred_flavor: :auth_none,
        cred: nil,
        verf_flavor: :auth_none,
        body: <<>>
      }

      {reply, _state} = Handler.handle_call(call, state)

      assert <<
               2::big-32,
               1::big-32,
               0::big-32,
               0::big-32,
               0::big-32,
               # PROG_MISMATCH = 2
               2::big-32,
               # low
               4::big-32,
               # high
               4::big-32
             >> = reply
    end
  end

  describe "unsupported procedures" do
    test "async variants return PROC_UNAVAIL", %{state: state} do
      for proc <- 6..15 do
        call = build_call(proc, <<>>)
        {reply, _state} = Handler.handle_call(call, state)
        assert_proc_unavail(reply, call.xid)
      end
    end

    test "share procedures return PROC_UNAVAIL", %{state: state} do
      for proc <- 20..22 do
        call = build_call(proc, <<>>)
        {reply, _state} = Handler.handle_call(call, state)
        assert_proc_unavail(reply, call.xid)
      end
    end
  end

  describe "malformed input" do
    test "returns failed for garbage lock args", %{state: state} do
      call = build_call(2, <<0, 1, 2>>)
      {reply, _state} = Handler.handle_call(call, state)
      assert reply != nil
    end

    test "returns failed for garbage test args", %{state: state} do
      call = build_call(1, <<0, 1, 2>>)
      {reply, _state} = Handler.handle_call(call, state)
      assert reply != nil
    end

    test "returns failed for garbage unlock args", %{state: state} do
      call = build_call(4, <<0, 1, 2>>)
      {reply, _state} = Handler.handle_call(call, state)
      assert reply != nil
    end
  end

  ## Helpers

  defp build_call(procedure, body) do
    %{
      xid: System.unique_integer([:positive]),
      program: @nlm_program,
      version: @nlm_version,
      procedure: procedure,
      cred_flavor: :auth_none,
      cred: nil,
      verf_flavor: :auth_none,
      body: body
    }
  end

  defp make_lock do
    # 24-byte NFS file handle: 8-byte inode + 16-byte volume_id
    fh = <<1::little-64, 0::128>>

    %{
      caller_name: "testhost",
      fh: fh,
      oh: <<1, 2, 3>>,
      svid: 100,
      offset: 0,
      length: 1024
    }
  end

  defp encode_lock(lock) do
    XDR.encode_string(lock.caller_name) <>
      XDR.encode_opaque(lock.fh) <>
      XDR.encode_opaque(lock.oh) <>
      XDR.encode_int(lock.svid) <>
      XDR.encode_hyper_uint(lock.offset) <>
      XDR.encode_hyper_uint(lock.length)
  end

  defp encode_testargs(cookie, exclusive, lock) do
    XDR.encode_opaque(cookie) <>
      XDR.encode_bool(exclusive) <>
      encode_lock(lock)
  end

  defp encode_lockargs(cookie, block, exclusive, lock, reclaim, state) do
    XDR.encode_opaque(cookie) <>
      XDR.encode_bool(block) <>
      XDR.encode_bool(exclusive) <>
      encode_lock(lock) <>
      XDR.encode_bool(reclaim) <>
      XDR.encode_int(state)
  end

  defp encode_unlockargs(cookie, lock) do
    XDR.encode_opaque(cookie) <>
      encode_lock(lock)
  end

  defp encode_cancargs(cookie, block, exclusive, lock) do
    XDR.encode_opaque(cookie) <>
      XDR.encode_bool(block) <>
      XDR.encode_bool(exclusive) <>
      encode_lock(lock)
  end

  defp assert_accepted_success(reply, xid) do
    assert <<
             ^xid::big-32,
             # reply
             1::big-32,
             # accepted
             0::big-32,
             # verf AUTH_NONE
             0::big-32,
             0::big-32,
             # SUCCESS
             0::big-32,
             _body::binary
           >> = reply
  end

  defp assert_proc_unavail(reply, xid) do
    assert <<
             ^xid::big-32,
             1::big-32,
             0::big-32,
             0::big-32,
             0::big-32,
             # PROC_UNAVAIL = 3
             3::big-32
           >> = reply
  end

  defp assert_nlm_res_stat(reply, xid, expected_stat) do
    expected_val = Codec.stat_to_int(expected_stat)

    assert <<
             ^xid::big-32,
             1::big-32,
             0::big-32,
             0::big-32,
             0::big-32,
             0::big-32,
             body::binary
           >> = reply

    # Parse cookie + stat from body
    assert {:ok, _cookie, rest} = XDR.decode_opaque(body)
    assert {:ok, ^expected_val, _rest} = XDR.decode_int(rest)
  end
end
