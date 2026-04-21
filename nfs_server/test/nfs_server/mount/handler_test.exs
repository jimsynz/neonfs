defmodule NFSServer.Mount.HandlerTest do
  use ExUnit.Case, async: true

  alias NFSServer.Mount.{Handler, Types}
  alias NFSServer.Mount.Types.{ExportNode, MountList}
  alias NFSServer.RPC.Auth
  alias NFSServer.XDR

  defmodule MemoryBackend do
    @moduledoc """
    In-memory backend used to exercise the MOUNT handler. Keyed on
    a process-dictionary so each test gets a clean slate.
    """

    @behaviour NFSServer.Mount.Backend

    @impl true
    def resolve("/exports/foo", _ctx), do: {:ok, "FH-FOO", [1]}
    def resolve("/exports/bar", _ctx), do: {:ok, "FH-BAR", [0, 1]}
    def resolve("/forbidden", _ctx), do: {:error, :acces}
    def resolve(_other, _ctx), do: {:error, :noent}

    @impl true
    def list_exports(_ctx) do
      [
        %ExportNode{dir: "/exports/foo", groups: ["everyone"]},
        %ExportNode{dir: "/exports/bar", groups: ["users", "admins"]}
      ]
    end

    @impl true
    def list_mounts(_ctx) do
      [
        %MountList{hostname: "client-a", directory: "/exports/foo"},
        %MountList{hostname: "client-b", directory: "/exports/bar"}
      ]
    end

    @impl true
    def record_mount(client, path, _auth) do
      send(self(), {:recorded, client, path})
      :ok
    end

    @impl true
    def forget_mount(client, path, _auth) do
      send(self(), {:forgot, client, path})
      :ok
    end

    @impl true
    def forget_all_mounts(client, _auth) do
      send(self(), {:forgot_all, client})
      :ok
    end
  end

  defp ctx, do: %{call: %{}, nfs_mount_backend: MemoryBackend}
  defp auth_sys(name \\ "host.example.com"), do: %Auth.Sys{machinename: name}

  describe "MOUNTPROC3_NULL" do
    test "ping returns an empty body" do
      assert {:ok, <<>>} = Handler.handle_call(0, <<>>, %Auth.None{}, ctx())
    end
  end

  describe "MOUNTPROC3_MNT" do
    test "successful mount returns OK + fhandle + auth flavours" do
      args = Types.encode_dirpath("/exports/foo")

      assert {:ok, body} = Handler.handle_call(1, args, auth_sys("alpha"), ctx())

      assert {:ok, :ok, rest} = Types.decode_stat(body)
      assert {:ok, "FH-FOO", rest2} = Types.decode_fhandle3(rest)
      assert {:ok, [1], <<>>} = XDR.decode_var_array(rest2, &XDR.decode_uint/1)

      assert_received {:recorded, "alpha", "/exports/foo"}
    end

    test "unknown export returns mountstat3 = NOENT" do
      args = Types.encode_dirpath("/missing")

      assert {:ok, body} = Handler.handle_call(1, args, auth_sys(), ctx())
      assert {:ok, :noent, <<>>} = Types.decode_stat(body)
      refute_received {:recorded, _, _}
    end

    test "permission failure returns mountstat3 = ACCES" do
      args = Types.encode_dirpath("/forbidden")
      assert {:ok, body} = Handler.handle_call(1, args, auth_sys(), ctx())
      assert {:ok, :acces, <<>>} = Types.decode_stat(body)
    end

    test "garbage args returns :garbage_args" do
      assert :garbage_args = Handler.handle_call(1, <<0xFF, 0xFF>>, auth_sys(), ctx())
    end
  end

  describe "MOUNTPROC3_DUMP" do
    test "lists every backend-reported mount as an XDR optional-data chain" do
      assert {:ok, body} = Handler.handle_call(2, <<>>, %Auth.None{}, ctx())

      assert {:ok, mounts, <<>>} = Types.decode_chain(body, &Types.decode_mountlist_entry/1)

      assert mounts == [
               %MountList{hostname: "client-a", directory: "/exports/foo"},
               %MountList{hostname: "client-b", directory: "/exports/bar"}
             ]
    end
  end

  describe "MOUNTPROC3_UMNT" do
    test "void reply, calls backend.forget_mount/3" do
      args = Types.encode_dirpath("/exports/foo")
      assert {:ok, <<>>} = Handler.handle_call(3, args, auth_sys("beta"), ctx())
      assert_received {:forgot, "beta", "/exports/foo"}
    end

    test "anonymous client when no AUTH_SYS machinename" do
      args = Types.encode_dirpath("/exports/bar")
      assert {:ok, <<>>} = Handler.handle_call(3, args, %Auth.None{}, ctx())
      assert_received {:forgot, "anonymous", "/exports/bar"}
    end

    test "garbage args returns :garbage_args" do
      assert :garbage_args = Handler.handle_call(3, <<0>>, auth_sys(), ctx())
    end
  end

  describe "MOUNTPROC3_UMNTALL" do
    test "void reply, calls backend.forget_all_mounts/2" do
      assert {:ok, <<>>} = Handler.handle_call(4, <<>>, auth_sys("gamma"), ctx())
      assert_received {:forgot_all, "gamma"}
    end
  end

  describe "MOUNTPROC3_EXPORT" do
    test "lists every export as an XDR optional-data chain" do
      assert {:ok, body} = Handler.handle_call(5, <<>>, %Auth.None{}, ctx())
      assert {:ok, exports, <<>>} = Types.decode_chain(body, &Types.decode_exportnode/1)

      assert exports == [
               %ExportNode{dir: "/exports/foo", groups: ["everyone"]},
               %ExportNode{dir: "/exports/bar", groups: ["users", "admins"]}
             ]
    end
  end

  describe "unknown procedures" do
    test "any proc not in 0..5 returns :proc_unavail" do
      for proc <- [6, 7, 99] do
        assert :proc_unavail = Handler.handle_call(proc, <<>>, %Auth.None{}, ctx())
      end
    end
  end

  describe "with_backend/1" do
    test "produces a registered handler module that re-enters the main handler with the backend in ctx" do
      bound = Handler.with_backend(MemoryBackend)
      args = Types.encode_dirpath("/exports/foo")
      ctx_no_backend = %{call: %{}}

      assert {:ok, body} = bound.handle_call(1, args, auth_sys(), ctx_no_backend)
      assert {:ok, :ok, _} = Types.decode_stat(body)
    end

    test "is idempotent — repeat calls return the same module" do
      assert Handler.with_backend(MemoryBackend) == Handler.with_backend(MemoryBackend)
    end
  end

  describe "missing backend in ctx" do
    test "raises a clear ArgumentError" do
      assert_raise ArgumentError, ~r/without a backend/, fn ->
        Handler.handle_call(5, <<>>, %Auth.None{}, %{call: %{}})
      end
    end
  end
end
