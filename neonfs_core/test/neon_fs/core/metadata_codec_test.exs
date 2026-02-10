defmodule NeonFS.Core.MetadataCodecTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.MetadataCodec

  @node_id :nonode@nohost

  describe "encode_record/1 and decode_record/1" do
    test "roundtrip with string value" do
      record = %{value: "hello", hlc_timestamp: {1000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert is_binary(binary)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == "hello"
      assert decoded.hlc_timestamp == {1000, 0, @node_id}
      assert decoded.tombstone == false
    end

    test "roundtrip with integer value" do
      record = %{value: 42, hlc_timestamp: {2000, 1, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == 42
    end

    test "roundtrip with nil value (tombstone)" do
      record = %{value: nil, hlc_timestamp: {3000, 0, @node_id}, tombstone: true}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == nil
      assert decoded.tombstone == true
    end

    test "roundtrip with map value (atom keys become strings)" do
      record = %{
        value: %{name: "test.txt", size: 1024},
        hlc_timestamp: {4000, 0, @node_id},
        tombstone: false
      }

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == %{"name" => "test.txt", "size" => 1024}
    end

    test "roundtrip with list value (atoms become strings)" do
      record = %{
        value: [1, "two", :three],
        hlc_timestamp: {5000, 0, @node_id},
        tombstone: false
      }

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == [1, "two", "three"]
    end

    test "roundtrip with binary value" do
      binary_data = :crypto.strong_rand_bytes(64)

      record = %{
        value: binary_data,
        hlc_timestamp: {6000, 0, @node_id},
        tombstone: false
      }

      assert {:ok, encoded} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(encoded)

      assert decoded.value == binary_data
    end

    test "roundtrip with empty map value" do
      record = %{value: %{}, hlc_timestamp: {7000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == %{}
    end

    test "roundtrip with nested map" do
      record = %{
        value: %{outer: %{inner: "deep"}},
        hlc_timestamp: {8000, 0, @node_id},
        tombstone: false
      }

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == %{"outer" => %{"inner" => "deep"}}
    end

    test "HLC node_id is preserved as atom" do
      record = %{value: "test", hlc_timestamp: {9000, 5, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      {_wall, _counter, node_id} = decoded.hlc_timestamp
      assert is_atom(node_id)
      assert node_id == @node_id
    end
  end

  describe "Tuple ext type" do
    test "roundtrip preserves tuples" do
      record = %{
        value: {:ok, "result", 42},
        hlc_timestamp: {1000, 0, @node_id},
        tombstone: false
      }

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == {"ok", "result", 42}
    end

    test "empty tuple" do
      record = %{value: {}, hlc_timestamp: {1000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == {}
    end

    test "two-element tuple" do
      record = %{value: {"key", "value"}, hlc_timestamp: {1000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == {"key", "value"}
    end
  end

  describe "MapSet ext type" do
    test "roundtrip preserves MapSets" do
      record = %{
        value: MapSet.new([1, 2, 3]),
        hlc_timestamp: {1000, 0, @node_id},
        tombstone: false
      }

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == MapSet.new([1, 2, 3])
    end

    test "empty MapSet" do
      record = %{value: MapSet.new(), hlc_timestamp: {1000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == MapSet.new()
    end
  end

  describe "Range ext type" do
    test "roundtrip preserves Ranges" do
      record = %{value: 1..10, hlc_timestamp: {1000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == 1..10
    end

    test "range with step" do
      record = %{value: 1..10//2, hlc_timestamp: {1000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == 1..10//2
    end

    test "decreasing range" do
      record = %{value: 10..1//-1, hlc_timestamp: {1000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == 10..1//-1
    end
  end

  describe "DateTime ext type" do
    test "roundtrip preserves DateTimes" do
      {:ok, dt, _} = DateTime.from_iso8601("2024-01-15T10:30:00Z")

      record = %{value: dt, hlc_timestamp: {1000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert DateTime.compare(decoded.value, dt) == :eq
    end
  end

  describe "nested structures" do
    test "map containing tuple containing list" do
      value = %{"data" => {1, [2, 3]}}

      record = %{value: value, hlc_timestamp: {1000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == %{"data" => {1, [2, 3]}}
    end

    test "list of tuples" do
      value = [{"a", 1}, {"b", 2}]

      record = %{value: value, hlc_timestamp: {1000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == [{"a", 1}, {"b", 2}]
    end

    test "deeply nested maps" do
      value = %{"a" => %{"b" => %{"c" => %{"d" => "deep"}}}}

      record = %{value: value, hlc_timestamp: {1000, 0, @node_id}, tombstone: false}

      assert {:ok, binary} = MetadataCodec.encode_record(record)
      assert {:ok, decoded} = MetadataCodec.decode_record(binary)

      assert decoded.value == value
    end
  end

  describe "decode_value/1" do
    test "passes through scalars" do
      assert MetadataCodec.decode_value(42) == 42
      assert MetadataCodec.decode_value("hello") == "hello"
      assert MetadataCodec.decode_value(true) == true
      assert MetadataCodec.decode_value(nil) == nil
    end

    test "recursively decodes maps" do
      assert MetadataCodec.decode_value(%{"a" => %{"b" => 1}}) == %{"a" => %{"b" => 1}}
    end

    test "recursively decodes lists" do
      assert MetadataCodec.decode_value([1, [2, 3]]) == [1, [2, 3]]
    end

    test "rejects non-NeonFS struct names" do
      data = %{"__struct__" => "Elixir.File.Stat", "size" => 0}

      # Should pass through as a plain map (no struct reconstruction)
      result = MetadataCodec.decode_value(data)
      assert is_map(result)
      assert result["__struct__"] == "Elixir.File.Stat"
    end
  end

  describe "error handling" do
    test "decode_record returns error for invalid binary" do
      assert {:error, _} = MetadataCodec.decode_record(<<0xFF, 0xFF>>)
    end

    test "decode_record returns error for non-binary" do
      assert {:error, _} = MetadataCodec.decode_record("not msgpax")
    end
  end
end
