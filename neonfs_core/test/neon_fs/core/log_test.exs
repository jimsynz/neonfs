defmodule NeonFS.Core.LogTest do
  use ExUnit.Case, async: true

  require Logger

  alias LoggerJSON.Formatters.Basic, as: BasicFormatter
  alias NeonFS.Core.Log

  describe "with_metadata/2" do
    test "sets metadata for the duration of the block" do
      Logger.metadata([])

      result =
        Log.with_metadata([component: "test.block", volume_id: "vol-1"], fn ->
          meta = Logger.metadata()
          assert meta[:component] == "test.block"
          assert meta[:volume_id] == "vol-1"
          :returned
        end)

      assert result == :returned
    end

    test "restores previous metadata after the block" do
      Logger.metadata(component: "original")

      Log.with_metadata([component: "temporary"], fn ->
        assert Logger.metadata()[:component] == "temporary"
      end)

      assert Logger.metadata()[:component] == "original"
    end

    test "restores metadata even when the block raises" do
      Logger.metadata(component: "before")

      assert_raise RuntimeError, fn ->
        Log.with_metadata([component: "during"], fn ->
          raise "boom"
        end)
      end

      assert Logger.metadata()[:component] == "before"
    end
  end

  describe "set_component/1" do
    test "sets the component metadata" do
      Log.set_component("cluster.join")
      assert Logger.metadata()[:component] == "cluster.join"
    end
  end

  describe "set_volume/1" do
    test "sets the volume_id metadata" do
      Log.set_volume("vol-abc")
      assert Logger.metadata()[:volume_id] == "vol-abc"
    end
  end

  describe "set_node_name/0" do
    test "sets node_name to the current node" do
      Log.set_node_name()
      assert Logger.metadata()[:node_name] == node()
    end
  end

  describe "set_request_id/0" do
    test "generates and sets a request_id" do
      request_id = Log.set_request_id()
      assert is_binary(request_id)
      assert String.length(request_id) > 0
      assert Logger.metadata()[:request_id] == request_id
    end

    test "generates unique IDs" do
      id1 = Log.set_request_id()
      id2 = Log.set_request_id()
      assert id1 != id2
    end
  end

  describe "set_request_id/1" do
    test "sets the given request_id" do
      Log.set_request_id("req-123")
      assert Logger.metadata()[:request_id] == "req-123"
    end
  end

  describe "JSON formatter configuration" do
    test "LoggerJSON.Formatters.Basic is available" do
      assert Code.ensure_loaded?(BasicFormatter)
    end

    test "formatter produces valid JSON" do
      {formatter_module, formatter_config} =
        BasicFormatter.new(metadata: [:component, :node_name, :volume_id, :request_id])

      log_event = %{
        level: :info,
        msg: {:string, "test message"},
        meta: %{
          time: System.system_time(:microsecond),
          component: "test",
          node_name: :nonode@nohost,
          volume_id: "vol-1",
          request_id: "req-abc"
        }
      }

      output =
        formatter_module.format(log_event, formatter_config)
        |> IO.iodata_to_binary()

      assert {:ok, decoded} = Jason.decode(output)
      assert decoded["message"] == "test message"
      assert decoded["metadata"]["component"] == "test"
      assert decoded["metadata"]["node_name"] == "nonode@nohost"
      assert decoded["metadata"]["volume_id"] == "vol-1"
      assert decoded["metadata"]["request_id"] == "req-abc"
      assert Map.has_key?(decoded, "time")
      assert Map.has_key?(decoded, "severity")
    end
  end
end
