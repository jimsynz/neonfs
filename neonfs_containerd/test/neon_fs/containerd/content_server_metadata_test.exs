defmodule NeonFS.Containerd.ContentServerMetadataTest do
  @moduledoc """
  Tests for the metadata RPCs (Info / List / Update / Delete) added
  in #551. Stubs the core RPC plane via the `:core_call_fn`
  Application env so the tests don't need a running cluster.
  """

  use ExUnit.Case, async: false

  alias Containerd.Services.Content.V1.{
    DeleteContentRequest,
    Info,
    InfoRequest,
    InfoResponse,
    ListContentRequest,
    ListContentResponse,
    UpdateRequest,
    UpdateResponse
  }

  alias GRPC.RPCError
  alias NeonFS.Containerd.ContentServer

  @valid_digest "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  @valid_path "sha256/01/23/456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

  setup do
    Application.put_env(:neonfs_containerd, :volume, "containerd-test")

    on_exit(fn ->
      Application.delete_env(:neonfs_containerd, :volume)
      Application.delete_env(:neonfs_containerd, :core_call_fn)
    end)

    :ok
  end

  defp stub_core(handler) when is_function(handler, 2) do
    Application.put_env(:neonfs_containerd, :core_call_fn, handler)
  end

  defp expect(parent, expected_fn, expected_args, response) do
    fn fn_name, args ->
      send(parent, {:core_call, fn_name, args})

      if fn_name == expected_fn and args == expected_args do
        response
      else
        {:error, {:unexpected_call, fn_name, args}}
      end
    end
  end

  defp file_meta(extras \\ %{}) do
    Map.merge(
      %{
        path: @valid_path,
        size: 42,
        created_at: DateTime.from_unix!(1_700_000_000, :second),
        updated_at: DateTime.from_unix!(1_700_000_500, :second),
        xattrs: %{"containerd.io/foo" => "bar", "user.unrelated" => "x"}
      },
      extras
    )
  end

  describe "Info" do
    test "returns digest, size, timestamps, and labels for a known blob" do
      stub_core(
        expect(self(), :get_file_meta, ["containerd-test", @valid_path], {:ok, file_meta()})
      )

      assert %InfoResponse{info: %Info{} = info} =
               ContentServer.info(%InfoRequest{digest: @valid_digest}, nil)

      assert info.digest == @valid_digest
      assert info.size == 42
      assert info.labels == %{"foo" => "bar"}
      assert info.created_at.seconds == 1_700_000_000
    end

    test "returns NOT_FOUND when the blob is missing" do
      stub_core(fn _, _ -> {:error, :not_found} end)

      assert_raise RPCError, fn ->
        ContentServer.info(%InfoRequest{digest: @valid_digest}, nil)
      end
    end

    test "returns INVALID_ARGUMENT for malformed digests" do
      assert_raise RPCError, fn ->
        ContentServer.info(%InfoRequest{digest: "sha256:short"}, nil)
      end
    end
  end

  describe "Update" do
    test "replaces all labels when the mask is empty" do
      parent = self()

      stub_core(fn
        :get_file_meta, _ ->
          {:ok, file_meta()}

        :update_file_meta, [_, _, opts] ->
          send(parent, {:updated_xattrs, Keyword.get(opts, :xattrs)})
          {:ok, file_meta(%{xattrs: Keyword.get(opts, :xattrs)})}
      end)

      assert %UpdateResponse{info: %Info{labels: labels}} =
               ContentServer.update(
                 %UpdateRequest{
                   info: %Info{digest: @valid_digest, labels: %{"new" => "value"}},
                   update_mask: nil
                 },
                 nil
               )

      assert labels == %{"new" => "value"}

      # Empty mask = full replace; the old "foo" label is gone, the
      # non-label "user.unrelated" xattr is preserved.
      assert_received {:updated_xattrs, xattrs}
      assert xattrs["containerd.io/new"] == "value"
      refute Map.has_key?(xattrs, "containerd.io/foo")
      assert xattrs["user.unrelated"] == "x"
    end

    test "honours a labels.<key> mask: sets one label, leaves others" do
      parent = self()

      stub_core(fn
        :get_file_meta, _ ->
          {:ok, file_meta()}

        :update_file_meta, [_, _, opts] ->
          send(parent, {:updated_xattrs, Keyword.get(opts, :xattrs)})
          {:ok, file_meta(%{xattrs: Keyword.get(opts, :xattrs)})}
      end)

      mask = %Google.Protobuf.FieldMask{paths: ["labels.added"]}

      ContentServer.update(
        %UpdateRequest{
          info: %Info{digest: @valid_digest, labels: %{"added" => "yes", "ignored" => "no"}},
          update_mask: mask
        },
        nil
      )

      assert_received {:updated_xattrs, xattrs}
      # "foo" preserved, "added" added, "ignored" not present (mask only mentioned "added")
      assert xattrs["containerd.io/foo"] == "bar"
      assert xattrs["containerd.io/added"] == "yes"
      refute Map.has_key?(xattrs, "containerd.io/ignored")
    end

    test "honours labels.<key> with empty value: clears that label" do
      parent = self()

      stub_core(fn
        :get_file_meta, _ ->
          {:ok, file_meta()}

        :update_file_meta, [_, _, opts] ->
          send(parent, {:updated_xattrs, Keyword.get(opts, :xattrs)})
          {:ok, file_meta(%{xattrs: Keyword.get(opts, :xattrs)})}
      end)

      mask = %Google.Protobuf.FieldMask{paths: ["labels.foo"]}

      ContentServer.update(
        %UpdateRequest{
          info: %Info{digest: @valid_digest, labels: %{"foo" => ""}},
          update_mask: mask
        },
        nil
      )

      assert_received {:updated_xattrs, xattrs}
      refute Map.has_key?(xattrs, "containerd.io/foo")
    end

    test "rejects an UpdateRequest with no Info payload" do
      assert_raise RPCError, ~r/Info payload/, fn ->
        ContentServer.update(%UpdateRequest{info: nil}, nil)
      end
    end
  end

  describe "List" do
    test "streams every sha256 blob under the configured volume" do
      stub_core(fn
        :list_files_recursive, ["containerd-test", "sha256/"] ->
          {:ok, [file_meta(), file_meta(%{path: @valid_path, size: 99})]}
      end)

      parent = self()
      ref = make_ref()
      stream = stream_capture(parent, ref)

      ContentServer.list(%ListContentRequest{filters: []}, stream)

      assert_received {^ref, %ListContentResponse{info: infos}}
      assert length(infos) == 2
      assert Enum.all?(infos, &match?(%Info{digest: @valid_digest}, &1))
    end

    test "skips entries whose path doesn't reverse-map to a digest" do
      stub_core(fn
        :list_files_recursive, _ ->
          {:ok, [file_meta(%{path: "sha256/_writes/something"}), file_meta()]}
      end)

      parent = self()
      ref = make_ref()
      stream = stream_capture(parent, ref)

      ContentServer.list(%ListContentRequest{filters: []}, stream)

      assert_received {^ref, %ListContentResponse{info: infos}}
      assert length(infos) == 1
    end

    test "returns UNIMPLEMENTED when filters are supplied" do
      assert_raise RPCError, ~r/label filters/, fn ->
        ContentServer.list(%ListContentRequest{filters: ["labels.foo==bar"]}, nil)
      end
    end
  end

  describe "Delete" do
    test "deletes the blob and returns Empty" do
      stub_core(expect(self(), :delete_file, ["containerd-test", @valid_path], :ok))

      assert %Google.Protobuf.Empty{} =
               ContentServer.delete(%DeleteContentRequest{digest: @valid_digest}, nil)

      assert_received {:core_call, :delete_file, _}
    end

    test "returns NOT_FOUND when the blob is missing" do
      stub_core(fn _, _ -> {:error, :not_found} end)

      assert_raise RPCError, fn ->
        ContentServer.delete(%DeleteContentRequest{digest: @valid_digest}, nil)
      end
    end

    test "returns INVALID_ARGUMENT for malformed digests" do
      assert_raise RPCError, fn ->
        ContentServer.delete(%DeleteContentRequest{digest: "nope"}, nil)
      end
    end
  end

  # Build a `GRPC.Server.Stream`-shaped struct sufficient for our
  # `send_reply/2` calls — the gRPC `:adapter` is delegated to but we
  # only ever forward one server-streamed message in the List RPC, so
  # we stub send_reply via a mocked adapter that messages the test.
  defp stream_capture(parent, ref) do
    %GRPC.Server.Stream{
      adapter: __MODULE__.StreamAdapter,
      __interface__: %{send_reply: &__MODULE__.StreamAdapter.send_reply/3},
      payload: {parent, ref}
    }
  end

  defmodule StreamAdapter do
    @moduledoc false
    def send_reply(%GRPC.Server.Stream{payload: {parent, ref}} = _stream, reply, _opts) do
      send(parent, {ref, reply})
      :ok
    end
  end
end
