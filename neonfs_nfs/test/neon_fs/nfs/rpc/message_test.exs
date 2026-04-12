defmodule NeonFS.NFS.RPC.MessageTest do
  use ExUnit.Case, async: true

  alias NeonFS.NFS.RPC.Message

  describe "decode_call" do
    test "decodes a valid RPC call with AUTH_NONE" do
      call =
        <<
          # xid
          42::big-32,
          # msg_type = CALL
          0::big-32,
          # rpc_vers = 2
          2::big-32,
          # program
          100_021::big-32,
          # version
          4::big-32,
          # procedure
          1::big-32,
          # cred: AUTH_NONE, length 0
          0::big-32,
          0::big-32,
          # verf: AUTH_NONE, length 0
          0::big-32,
          0::big-32,
          # body
          "body"::binary
        >>

      assert {:ok, parsed} = Message.decode_call(call)
      assert parsed.xid == 42
      assert parsed.program == 100_021
      assert parsed.version == 4
      assert parsed.procedure == 1
      assert parsed.cred_flavor == :auth_none
      assert parsed.body == "body"
    end

    test "decodes a valid RPC call with AUTH_SYS" do
      auth_body =
        <<
          # stamp
          12_345::big-32,
          # machine_name "host" (length 4, no padding needed)
          4::big-32,
          "host"::binary,
          # uid
          1000::big-32,
          # gid
          1000::big-32,
          # gids count = 2
          2::big-32,
          100::big-32,
          200::big-32
        >>

      call =
        <<
          # xid
          99::big-32,
          # msg_type = CALL
          0::big-32,
          # rpc_vers = 2
          2::big-32,
          # program
          100_021::big-32,
          # version
          4::big-32,
          # procedure
          2::big-32,
          # cred: AUTH_SYS
          1::big-32,
          byte_size(auth_body)::big-32,
          auth_body::binary,
          # verf: AUTH_NONE, length 0
          0::big-32,
          0::big-32
        >>

      assert {:ok, parsed} = Message.decode_call(call)
      assert parsed.xid == 99
      assert parsed.cred_flavor == :auth_sys
      assert parsed.cred.uid == 1000
      assert parsed.cred.gid == 1000
      assert parsed.cred.machine_name == "host"
      assert parsed.cred.gids == [100, 200]
    end

    test "rejects non-CALL messages" do
      reply =
        <<
          42::big-32,
          # msg_type = REPLY
          1::big-32,
          2::big-32,
          0::big-32,
          0::big-32,
          0::big-32,
          0::big-32,
          0::big-32,
          0::big-32,
          0::big-32
        >>

      assert {:error, :not_a_call} = Message.decode_call(reply)
    end

    test "rejects wrong RPC version" do
      call =
        <<
          42::big-32,
          0::big-32,
          # rpc_vers = 3 (wrong)
          3::big-32,
          0::big-32,
          0::big-32,
          0::big-32,
          0::big-32,
          0::big-32,
          0::big-32,
          0::big-32
        >>

      assert {:error, :rpc_mismatch} = Message.decode_call(call)
    end
  end

  describe "encode_accepted_reply" do
    test "encodes a success reply" do
      body = <<42::big-32>>
      reply = Message.encode_accepted_reply(1, :success, body)

      assert <<
               # xid
               1::big-32,
               # reply
               1::big-32,
               # accepted
               0::big-32,
               # verf AUTH_NONE
               0::big-32,
               0::big-32,
               # SUCCESS
               0::big-32,
               # body
               42::big-32
             >> = reply
    end

    test "encodes PROC_UNAVAIL reply" do
      reply = Message.encode_accepted_reply(5, :proc_unavail)

      assert <<
               5::big-32,
               1::big-32,
               0::big-32,
               0::big-32,
               0::big-32,
               # PROC_UNAVAIL = 3
               3::big-32
             >> = reply
    end

    test "encodes SYSTEM_ERR reply" do
      reply = Message.encode_accepted_reply(7, :system_err)

      assert <<
               7::big-32,
               1::big-32,
               0::big-32,
               0::big-32,
               0::big-32,
               # SYSTEM_ERR = 5
               5::big-32
             >> = reply
    end
  end

  describe "encode_prog_mismatch" do
    test "includes low and high version numbers" do
      reply = Message.encode_prog_mismatch(10, 4, 4)

      assert <<
               10::big-32,
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

  describe "encode_rejected_reply" do
    test "encodes an auth error reply" do
      reply = Message.encode_rejected_reply(3, 1)

      assert <<
               3::big-32,
               1::big-32,
               # rejected
               1::big-32,
               # AUTH_ERROR
               1::big-32,
               # auth_stat
               1::big-32
             >> = reply
    end
  end
end
