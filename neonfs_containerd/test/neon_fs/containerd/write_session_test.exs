defmodule NeonFS.Containerd.WriteSessionTest do
  @moduledoc """
  Unit tests for `WriteSession`. Covers the feed/stat/commit/abort
  flow, hash verification on commit, offset-mismatch protection on
  resume, and the streaming-invariant assertion (per-frame buffer
  bounded by data segment, not by total blob size).
  """

  use ExUnit.Case, async: false

  alias NeonFS.Containerd.{StubChunkWriter, WriteRegistry, WriteSession, WriteSupervisor}

  setup do
    StubChunkWriter.start({:ok, []})
    ensure_registry()
    ensure_supervisor()

    parent = self()

    Application.put_env(:neonfs_containerd, :volume, "containerd-test")

    Application.put_env(:neonfs_containerd, :commit_chunks_fn, fn vol, path, hashes, extra ->
      send(parent, {:commit_chunks_called, vol, path, hashes, extra})
      {:ok, %{path: path}}
    end)

    on_exit(fn ->
      StubChunkWriter.stop()
      Application.delete_env(:neonfs_containerd, :volume)
      Application.delete_env(:neonfs_containerd, :commit_chunks_fn)
    end)

    :ok
  end

  defp ensure_registry do
    case Process.whereis(WriteRegistry) do
      nil -> {:ok, _} = Registry.start_link(keys: :unique, name: WriteRegistry)
      _ -> :ok
    end
  end

  defp ensure_supervisor do
    case Process.whereis(WriteSupervisor) do
      nil -> {:ok, _} = WriteSupervisor.start_link([])
      _ -> :ok
    end
  end

  defp start_session(ref, opts \\ []) do
    opts = Keyword.merge([ref: ref, chunk_writer_module: StubChunkWriter], opts)
    WriteSupervisor.start_session(ref, opts)
  end

  defp drain_session(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      _ = WriteSession.abort(pid)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        500 -> :ok
      end
    end
  end

  describe "feed/2" do
    test "tracks offset across multiple feeds and ships every byte to the chunk writer" do
      {:ok, pid} = start_session("ref-feed-1")

      assert {:ok, 5} = WriteSession.feed(pid, "alpha")
      assert {:ok, 9} = WriteSession.feed(pid, "beta")
      assert {:ok, 14} = WriteSession.feed(pid, "gamma")

      drain_session(pid)
      assert StubChunkWriter.collected_segments() == ["alpha", "beta", "gamma"]
    end

    test "rejects a feed whose expected_offset doesn't match" do
      {:ok, pid} = start_session("ref-feed-mismatch")

      assert {:ok, 5} = WriteSession.feed(pid, "alpha")
      assert {:error, :offset_mismatch} = WriteSession.feed(pid, "beta", 999)

      drain_session(pid)
    end
  end

  describe "stat/1" do
    test "returns the running offset / total" do
      {:ok, pid} = start_session("ref-stat-1", total: 1024)

      assert %{offset: 0, total: 1024} = WriteSession.stat(pid)

      WriteSession.feed(pid, :binary.copy(<<0>>, 256))

      assert %{offset: 256, total: 1024} = WriteSession.stat(pid)

      drain_session(pid)
    end
  end

  describe "commit/2" do
    test "verifies digest, calls commit_chunks, returns the digest on success" do
      payload = "hello world"
      digest = "sha256:" <> Base.encode16(:crypto.hash(:sha256, payload), case: :lower)

      ref_struct = %{
        hash: "h1",
        locations: [],
        size: byte_size(payload),
        codec: %{compression: :none, crypto: nil, original_size: byte_size(payload)}
      }

      StubChunkWriter.start({:ok, [ref_struct]})

      {:ok, pid} = start_session("ref-commit-ok")
      WriteSession.feed(pid, payload)

      assert {:ok, %{digest: ^digest, offset: 11}} = WriteSession.commit(pid, digest)
      refute Process.alive?(pid)

      assert_received {:commit_chunks_called, "containerd-test", path, ["h1"], _opts}
      assert path =~ "sha256/"
    end

    test "returns :digest_mismatch when computed and expected hashes differ" do
      payload = "hello world"
      wrong_digest = "sha256:" <> String.duplicate("0", 64)

      ref_struct = %{
        hash: "h1",
        locations: [],
        size: byte_size(payload),
        codec: %{compression: :none, crypto: nil, original_size: byte_size(payload)}
      }

      StubChunkWriter.start({:ok, [ref_struct]})

      {:ok, pid} = start_session("ref-commit-mismatch")
      WriteSession.feed(pid, payload)

      assert {:error, :digest_mismatch} = WriteSession.commit(pid, wrong_digest)
      refute Process.alive?(pid)

      refute_received {:commit_chunks_called, _, _, _, _}
    end

    test "returns :digest_mismatch when expected is empty" do
      StubChunkWriter.start({:ok, []})
      {:ok, pid} = start_session("ref-commit-empty")

      assert {:error, :digest_mismatch} = WriteSession.commit(pid, "")
      refute Process.alive?(pid)
    end
  end

  describe "abort/1" do
    test "stops the session cleanly" do
      {:ok, pid} = start_session("ref-abort-1")

      WriteSession.feed(pid, "some-bytes")
      assert :ok = WriteSession.abort(pid)
      refute Process.alive?(pid)
    end
  end

  describe "resume" do
    test "start_session returns the existing pid on a second call with the same ref" do
      {:ok, pid_1} = start_session("ref-resume")
      {:ok, pid_2} = start_session("ref-resume")

      assert pid_1 == pid_2

      WriteSession.feed(pid_1, "first")
      assert %{offset: 5} = WriteSession.stat(pid_2)

      drain_session(pid_1)
    end

    test "second connection with non-zero offset picks up where the first stopped" do
      payload_1 = "first-half-"
      payload_2 = "second-half"
      full = payload_1 <> payload_2
      digest = "sha256:" <> Base.encode16(:crypto.hash(:sha256, full), case: :lower)

      ref_struct = %{
        hash: "h1",
        locations: [],
        size: byte_size(full),
        codec: %{compression: :none, crypto: nil, original_size: byte_size(full)}
      }

      StubChunkWriter.start({:ok, [ref_struct]})

      {:ok, pid} = start_session("ref-resume-flow")
      WriteSession.feed(pid, payload_1)

      # "Disconnect" — pid stays alive because the GenServer survives the
      # bidi-stream end. Resume hands the same ref back.
      {:ok, ^pid} = start_session("ref-resume-flow")

      # Containerd's resume sends an offset matching where it left off.
      assert {:ok, _} = WriteSession.feed(pid, payload_2, byte_size(payload_1))

      assert {:ok, %{digest: ^digest}} = WriteSession.commit(pid, digest)
    end
  end

  describe "streaming invariant" do
    test "feeding a 10 MiB payload as 1 MiB segments doesn't accumulate the whole blob in the inbox" do
      {:ok, pid} = start_session("ref-stream-invariant")

      Enum.each(1..10, fn _ ->
        WriteSession.feed(pid, :binary.copy(<<0>>, 1024 * 1024))
      end)

      expected_total = 10 * 1024 * 1024
      assert %{offset: ^expected_total} = WriteSession.stat(pid)

      drain_session(pid)
      max_seg = StubChunkWriter.collected_segments() |> Enum.map(&byte_size/1) |> Enum.max()
      assert max_seg == 1024 * 1024
    end
  end
end
