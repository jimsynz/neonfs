defmodule NeonFS.CIFS.ListenerTest do
  use ExUnit.Case, async: true

  alias NeonFS.CIFS.Listener

  doctest Listener

  describe "encode/1" do
    test "prefixes a 4-byte big-endian length to the ETF body" do
      iodata = Listener.encode({:hello, "world"})
      bytes = IO.iodata_to_binary(iodata)

      <<len::big-32, body::binary>> = bytes
      assert byte_size(body) == len
      assert :erlang.binary_to_term(body) == {:hello, "world"}
    end

    test "round-trips a complex term verbatim through decode/1" do
      term = %{volume: "vol-a", entries: [{1, "a"}, {2, "b"}], eof: true}
      <<_len::big-32, body::binary>> = IO.iodata_to_binary(Listener.encode(term))
      assert {:ok, ^term} = Listener.decode(body)
    end
  end

  describe "decode/1" do
    test "returns :badetf on garbage" do
      assert {:error, :badetf} = Listener.decode(<<0, 0, 0, 0>>)
    end
  end
end
