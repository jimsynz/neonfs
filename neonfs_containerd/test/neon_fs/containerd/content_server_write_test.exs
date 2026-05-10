defmodule NeonFS.Containerd.ContentServerWriteTest do
  @moduledoc """
  Tests for the `Write` bidi-streaming RPC dispatch state machine
  (#729). Goes through the test entry point
  `ContentServer.process_write_stream/2` so a real
  `GRPC.Server.Stream` isn't required.

  The headline regression that motivated this module: the original
  `WRITE` action handler updated session state but did not send a
  reply back to the caller. Real `containerd` clients block on the
  per-frame response when streaming larger blobs through the proxy
  plugin, so a missing `WRITE` reply manifests as a hung
  `ctr content ingest`.
  """

  use ExUnit.Case, async: false

  alias Containerd.Services.Content.V1.{WriteContentRequest, WriteContentResponse}
  alias NeonFS.Containerd.{ContentServer, StubChunkWriter, WriteRegistry}

  @volume "containerd-test"

  setup do
    StubChunkWriter.start(
      {:ok,
       [
         %{
           hash: "h1",
           locations: [],
           size: 0,
           codec: %{compression: :none, crypto: nil, original_size: 0}
         }
       ]}
    )

    Application.put_env(:neonfs_containerd, :chunk_writer_module, StubChunkWriter)
    Application.put_env(:neonfs_containerd, :volume, @volume)
    Application.put_env(:neonfs_containerd, :default_volume, @volume)

    Application.put_env(:neonfs_containerd, :commit_chunks_fn, fn _vol, path, _hashes, _extra ->
      {:ok, %{path: path}}
    end)

    ref = "test-ref-#{System.unique_integer([:positive])}"

    on_exit(fn ->
      cleanup_session(ref)
      StubChunkWriter.stop()
      Application.delete_env(:neonfs_containerd, :chunk_writer_module)
      Application.delete_env(:neonfs_containerd, :volume)
      Application.delete_env(:neonfs_containerd, :default_volume)
      Application.delete_env(:neonfs_containerd, :commit_chunks_fn)
    end)

    {:ok, ref: ref}
  end

  describe "process_write_stream — STAT action" do
    test "sends a STAT response with the freshly-created session's offset", %{ref: ref} do
      replies = capture_replies([%WriteContentRequest{action: :STAT, ref: ref}])

      assert [%WriteContentResponse{action: :STAT, offset: 0, total: 0}] = replies
    end
  end

  describe "process_write_stream — WRITE action (#729)" do
    test "sends a WRITE response per frame so the client doesn't block", %{ref: ref} do
      payload = "first batch of bytes"

      replies =
        capture_replies([
          %WriteContentRequest{
            action: :WRITE,
            ref: ref,
            data: payload,
            offset: 0
          }
        ])

      assert [%WriteContentResponse{action: :WRITE, offset: offset}] = replies
      assert offset == byte_size(payload)
    end

    test "subsequent WRITE frames carry cumulative offsets", %{ref: ref} do
      first = "alpha"
      second = "beta-gamma"

      replies =
        capture_replies([
          %WriteContentRequest{action: :WRITE, ref: ref, data: first, offset: 0},
          %WriteContentRequest{
            action: :WRITE,
            ref: ref,
            data: second,
            offset: byte_size(first)
          }
        ])

      assert [
               %WriteContentResponse{action: :WRITE, offset: o1},
               %WriteContentResponse{action: :WRITE, offset: o2}
             ] = replies

      assert o1 == byte_size(first)
      assert o2 == byte_size(first) + byte_size(second)
    end
  end

  describe "process_write_stream — STAT carries total and expected (#741)" do
    test "STAT request's `total` populates the session", %{ref: ref} do
      [reply] =
        capture_replies([
          %WriteContentRequest{action: :STAT, ref: ref, total: 617, expected: "sha256:abc"}
        ])

      assert %WriteContentResponse{action: :STAT, total: 617} = reply
    end

    test "STAT request's `expected` carries through to the session's Status RPC", %{ref: ref} do
      capture_replies([
        %WriteContentRequest{action: :STAT, ref: ref, total: 617, expected: "sha256:def"}
      ])

      # The session's expected digest is exposed via a follow-up STAT
      # which would otherwise not pick up `expected` on its own (the
      # snapshot doesn't carry it). For coverage we lookup the session
      # directly to assert the field is set on its state.
      {:ok, pid} = WriteRegistry.lookup(ref)
      assert :sys.get_state(pid).expected == "sha256:def"
    end
  end

  describe "process_write_stream — telemetry (#741)" do
    setup do
      tel_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :containerd, :write_frame]
        ])

      {:ok, tel_ref: tel_ref}
    end

    test "emits a frame event per STAT/WRITE/COMMIT round-trip", %{ref: ref, tel_ref: tel_ref} do
      payload = "two-frame-blob"
      digest = "sha256:" <> (:crypto.hash(:sha256, payload) |> Base.encode16(case: :lower))

      capture_replies([
        %WriteContentRequest{action: :STAT, ref: ref, total: byte_size(payload)},
        %WriteContentRequest{
          action: :WRITE,
          ref: ref,
          data: payload,
          offset: 0,
          total: byte_size(payload)
        },
        %WriteContentRequest{
          action: :COMMIT,
          ref: ref,
          offset: byte_size(payload),
          total: byte_size(payload),
          expected: digest
        }
      ])

      assert_received {[:neonfs, :containerd, :write_frame], ^tel_ref,
                       %{req_offset: 0, req_data_size: 0, resp_offset: 0},
                       %{action: :STAT, ref: ^ref, result: :ok}}

      assert_received {[:neonfs, :containerd, :write_frame], ^tel_ref,
                       %{req_offset: 0, req_data_size: 14, resp_offset: 14},
                       %{action: :WRITE, ref: ^ref, result: :ok}}

      assert_received {[:neonfs, :containerd, :write_frame], ^tel_ref,
                       %{req_offset: 14, req_data_size: 0, resp_offset: 14},
                       %{action: :COMMIT, ref: ^ref, result: :ok, digest: ^digest}}
    end

    test "emits an :error event with session_offset when WRITE arrives at the wrong offset",
         %{ref: ref, tel_ref: tel_ref} do
      try do
        capture_replies([
          %WriteContentRequest{action: :WRITE, ref: ref, data: "x", offset: 999}
        ])
      rescue
        GRPC.RPCError -> :ok
      end

      assert_received {[:neonfs, :containerd, :write_frame], ^tel_ref,
                       %{req_offset: 999, req_data_size: 1, session_offset: 0},
                       %{action: :WRITE, ref: ^ref, result: :error, reason: :offset_mismatch}}
    end
  end

  describe "process_write_stream — full STAT → WRITE → COMMIT cycle" do
    test "emits one response per request, in order, with the COMMIT digest", %{ref: ref} do
      payload = "the entire blob in one frame"
      digest = "sha256:" <> (:crypto.hash(:sha256, payload) |> Base.encode16(case: :lower))

      replies =
        capture_replies([
          %WriteContentRequest{action: :STAT, ref: ref},
          %WriteContentRequest{
            action: :WRITE,
            ref: ref,
            data: payload,
            offset: 0,
            total: byte_size(payload)
          },
          %WriteContentRequest{
            action: :COMMIT,
            ref: ref,
            offset: byte_size(payload),
            total: byte_size(payload),
            expected: digest
          }
        ])

      assert [
               %WriteContentResponse{action: :STAT},
               %WriteContentResponse{action: :WRITE, offset: write_offset},
               %WriteContentResponse{action: :COMMIT, digest: ^digest}
             ] = replies

      assert write_offset == byte_size(payload)
    end
  end

  ## Helpers

  defp capture_replies(requests) do
    parent = self()
    ref = make_ref()
    send_fn = fn reply -> send(parent, {ref, reply}) end

    ContentServer.process_write_stream(requests, send_fn)

    drain_replies(ref, [])
  end

  defp drain_replies(ref, acc) do
    receive do
      {^ref, reply} -> drain_replies(ref, [reply | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # Synchronous cleanup so the next test (or ContentServerTest, which
  # asserts an empty registry) sees a clean WriteRegistry. `Process.exit`
  # is async; the registry's auto-unregister fires off the EXIT signal
  # and races the next setup. Monitor-and-await deflakes the suite.
  defp cleanup_session(ref) do
    case WriteRegistry.lookup(ref) do
      {:ok, pid} when is_pid(pid) ->
        if Process.alive?(pid) do
          mref = Process.monitor(pid)
          Process.exit(pid, :kill)

          receive do
            {:DOWN, ^mref, :process, ^pid, _} -> :ok
          after
            1_000 -> :ok
          end
        end

      :error ->
        :ok
    end
  end
end
