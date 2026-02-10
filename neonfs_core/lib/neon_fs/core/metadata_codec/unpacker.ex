defmodule NeonFS.Core.MetadataCodec.Unpacker do
  @moduledoc """
  Handles MessagePack extension types during unpacking.

  ## Extension Type IDs

  | ID | Type   | Packed as                         |
  |----|--------|-----------------------------------|
  | 1  | Tuple  | msgpax-packed list of elements    |
  | 2  | MapSet | msgpax-packed sorted list         |
  | 3  | Range  | msgpax-packed [first, last, step] |

  DateTime is handled by msgpax's built-in ReservedExt (ext type -1).
  """

  @behaviour Msgpax.Ext.Unpacker

  @impl true
  def unpack(%Msgpax.Ext{type: 1, data: data}) do
    {:ok, data |> Msgpax.unpack!() |> List.to_tuple()}
  end

  def unpack(%Msgpax.Ext{type: 2, data: data}) do
    {:ok, data |> Msgpax.unpack!() |> MapSet.new()}
  end

  def unpack(%Msgpax.Ext{type: 3, data: data}) do
    [first, last, step] = Msgpax.unpack!(data)
    {:ok, Range.new(first, last, step)}
  end

  def unpack(ext), do: {:ok, ext}
end
