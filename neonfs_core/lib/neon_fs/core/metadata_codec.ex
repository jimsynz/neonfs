defmodule NeonFS.Core.MetadataCodec do
  @moduledoc """
  MessagePack-based serialisation for metadata records.

  Replaces `:erlang.term_to_binary` / `binary_to_term` with msgpax, providing a
  constrained type system that cannot produce dangerous terms regardless of input.

  ## Record Envelope

  Records are packed as compact maps:

      %{"v" => value, "ts" => [wall_ms, counter, node_string], "t" => tombstone}

  ## Value Transformations

  - Atom values become strings (`:hot` → `"hot"`)
  - Atom map keys become string keys (`%{name: "x"}` → `%{"name" => "x"}`)
  - Tuples, MapSets, Ranges are preserved via MessagePack Ext types
  - DateTimes are preserved via msgpax's built-in timestamp extension
  - Structs with `@derive [{Msgpax.Packer, include_struct_field: true}]` are
    fully reconstructed on decode (restricted to `NeonFS.*` modules)
  """

  alias NeonFS.Core.MetadataCodec.Unpacker

  @doc """
  Encodes a metadata record to a MessagePack binary.

  The record must be a map with `:value`, `:hlc_timestamp`, and `:tombstone` keys.
  """
  @spec encode_record(map()) :: {:ok, binary()} | {:error, term()}
  def encode_record(%{value: value, hlc_timestamp: {wall, counter, node}, tombstone: tombstone}) do
    envelope = %{
      "v" => value,
      "ts" => [wall, counter, Atom.to_string(node)],
      "t" => tombstone
    }

    {:ok, Msgpax.pack!(envelope) |> IO.iodata_to_binary()}
  rescue
    e -> {:error, e}
  end

  @doc """
  Decodes a MessagePack binary back into a metadata record.
  """
  @spec decode_record(binary()) :: {:ok, map()} | {:error, term()}
  def decode_record(data) when is_binary(data) do
    %{"v" => raw_value, "ts" => [wall, counter, node_str], "t" => tombstone} =
      Msgpax.unpack!(data, ext: Unpacker)

    record = %{
      value: decode_value(raw_value),
      hlc_timestamp: {wall, counter, String.to_existing_atom(node_str)},
      tombstone: tombstone
    }

    {:ok, record}
  rescue
    e -> {:error, e}
  end

  @doc """
  Recursively walks decoded msgpax data, reconstructing structs from
  maps containing a `"__struct__"` key.

  Only `NeonFS.*` module names are accepted for struct reconstruction.
  """
  @spec decode_value(term()) :: term()
  def decode_value(%{"__struct__" => "Elixir.NeonFS." <> _ = mod_str} = data) do
    module = String.to_existing_atom(mod_str)

    attrs =
      data
      |> Map.delete("__struct__")
      |> Map.new(fn {k, v} -> {String.to_existing_atom(k), decode_value(v)} end)

    struct!(module, attrs)
  end

  def decode_value(value) when is_struct(value), do: value

  def decode_value(%{} = map) do
    Map.new(map, fn {k, v} -> {k, decode_value(v)} end)
  end

  def decode_value(list) when is_list(list) do
    Enum.map(list, &decode_value/1)
  end

  def decode_value(value), do: value
end
