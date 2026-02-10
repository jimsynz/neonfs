defimpl Msgpax.Packer, for: Tuple do
  def pack(tuple) do
    Msgpax.Ext.new(1, Msgpax.pack!(Tuple.to_list(tuple)))
    |> @protocol.pack()
  end
end

defimpl Msgpax.Packer, for: MapSet do
  def pack(mapset) do
    Msgpax.Ext.new(2, Msgpax.pack!(mapset |> MapSet.to_list() |> Enum.sort()))
    |> @protocol.pack()
  end
end

defimpl Msgpax.Packer, for: Range do
  def pack(first..last//step) do
    Msgpax.Ext.new(3, Msgpax.pack!([first, last, step]))
    |> @protocol.pack()
  end
end
