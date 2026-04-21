defmodule NFSServer.XDRTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NFSServer.XDR

  doctest XDR

  describe "int (§4.1)" do
    property "round-trips any 32-bit signed integer" do
      check all(n <- integer(-0x80000000..0x7FFFFFFF)) do
        assert {:ok, ^n, <<>>} = n |> XDR.encode_int() |> XDR.decode_int()
      end
    end

    test "encodes 1 as `0x00 0x00 0x00 0x01` (big-endian)" do
      assert XDR.encode_int(1) == <<0, 0, 0, 1>>
    end

    test "encodes -1 as 0xFFFFFFFF (two's complement, big-endian)" do
      assert XDR.encode_int(-1) == <<0xFF, 0xFF, 0xFF, 0xFF>>
    end

    test "decode returns :short_binary on fewer than 4 bytes" do
      assert {:error, :short_binary} = XDR.decode_int(<<0, 0, 0>>)
    end
  end

  describe "uint (§4.2)" do
    property "round-trips any 32-bit unsigned integer" do
      check all(n <- integer(0..0xFFFFFFFF)) do
        assert {:ok, ^n, <<>>} = n |> XDR.encode_uint() |> XDR.decode_uint()
      end
    end

    test "encodes 0xDEADBEEF exactly" do
      assert XDR.encode_uint(0xDEADBEEF) == <<0xDE, 0xAD, 0xBE, 0xEF>>
    end
  end

  describe "hyper / unsigned hyper (§4.5)" do
    property "hyper round-trips any 64-bit signed integer" do
      check all(n <- integer(-0x80000000_00000000..0x7FFFFFFF_FFFFFFFF)) do
        assert {:ok, ^n, <<>>} = n |> XDR.encode_hyper() |> XDR.decode_hyper()
      end
    end

    property "uhyper round-trips any 64-bit unsigned integer" do
      check all(n <- integer(0..0xFFFFFFFF_FFFFFFFF)) do
        assert {:ok, ^n, <<>>} = n |> XDR.encode_uhyper() |> XDR.decode_uhyper()
      end
    end

    test "hyper encodes 1 as 8 bytes big-endian" do
      assert XDR.encode_hyper(1) == <<0, 0, 0, 0, 0, 0, 0, 1>>
    end
  end

  describe "float / double (§4.6, §4.7)" do
    property "float round-trips" do
      check all(f <- float(min: -1.0e10, max: 1.0e10)) do
        assert {:ok, decoded, <<>>} = f |> XDR.encode_float() |> XDR.decode_float()
        # single-precision loses fidelity; compare loosely
        assert_in_delta decoded, f, abs(f) * 1.0e-6 + 1.0e-30
      end
    end

    property "double round-trips exactly" do
      check all(f <- float(min: -1.0e100, max: 1.0e100)) do
        assert {:ok, ^f, <<>>} = f |> XDR.encode_double() |> XDR.decode_double()
      end
    end
  end

  describe "bool (§4.4)" do
    test "encodes true as 1 and false as 0" do
      assert XDR.encode_bool(true) == <<0, 0, 0, 1>>
      assert XDR.encode_bool(false) == <<0, 0, 0, 0>>
    end

    test "decodes 0 / 1 back to false / true" do
      assert {:ok, false, <<>>} = XDR.decode_bool(<<0::32>>)
      assert {:ok, true, <<>>} = XDR.decode_bool(<<1::32>>)
    end

    test "rejects any discriminant other than 0 / 1" do
      assert {:error, {:bad_bool, 2}} = XDR.decode_bool(<<2::32>>)
    end
  end

  describe "enum (§4.3)" do
    property "round-trips as a signed int" do
      check all(n <- integer(-100..100)) do
        assert {:ok, ^n, <<>>} = n |> XDR.encode_enum() |> XDR.decode_enum()
      end
    end
  end

  describe "void (§4.16)" do
    test "encode_void/1 returns <<>> regardless of argument" do
      assert XDR.encode_void(:anything) == <<>>
      assert XDR.encode_void(nil) == <<>>
    end

    test "decode_void/1 consumes zero bytes" do
      assert {:ok, :void, <<1, 2, 3>>} = XDR.decode_void(<<1, 2, 3>>)
    end
  end

  describe "fixed-length opaque (§4.9)" do
    property "round-trips with 0–3 padding bytes to hit a 4-byte boundary" do
      check all(
              size <- integer(0..16),
              data <- binary(length: size)
            ) do
        encoded = XDR.encode_fixed_opaque(data, size)
        # encoded length must be a multiple of 4
        assert rem(byte_size(encoded), 4) == 0
        # decoding returns the original data + empty rest
        assert {:ok, ^data, <<>>} = XDR.decode_fixed_opaque(encoded, size)
      end
    end

    test "decode rejects non-zero padding bytes" do
      assert {:error, {:bad_pad, <<1>>}} =
               XDR.decode_fixed_opaque(<<0x41, 0x42, 0x43, 0x01>>, 3)
    end
  end

  describe "variable-length opaque / string (§4.10, §4.11)" do
    property "variable opaque round-trips" do
      check all(data <- binary(max_length: 32)) do
        encoded = XDR.encode_var_opaque(data)
        assert rem(byte_size(encoded), 4) == 0
        assert {:ok, ^data, <<>>} = XDR.decode_var_opaque(encoded)
      end
    end

    property "string uses the same wire format as var opaque" do
      check all(s <- binary(max_length: 32)) do
        assert XDR.encode_string(s) == XDR.encode_var_opaque(s)
      end
    end

    test "example from RFC 4506 §4.10 — a 5-byte opaque `hello`" do
      # Length prefix 5 + 'h' 'e' 'l' 'l' 'o' + 3 bytes of zero padding
      assert XDR.encode_var_opaque("hello") ==
               <<0, 0, 0, 5, ?h, ?e, ?l, ?l, ?o, 0, 0, 0>>
    end
  end

  describe "fixed and variable arrays (§4.12, §4.13)" do
    property "fixed array round-trips a known length" do
      check all(list <- list_of(integer(-100..100), length: 4)) do
        encoded = XDR.encode_fixed_array(list, 4, &XDR.encode_int/1)
        assert byte_size(encoded) == 4 * 4

        assert {:ok, ^list, <<>>} =
                 XDR.decode_fixed_array(encoded, 4, &XDR.decode_int/1)
      end
    end

    test "fixed array raises on mismatched length" do
      assert_raise ArgumentError, fn ->
        XDR.encode_fixed_array([1, 2, 3], 4, &XDR.encode_int/1)
      end
    end

    property "variable array round-trips with its count prefix" do
      check all(list <- list_of(integer(0..1_000_000), max_length: 8)) do
        encoded = XDR.encode_var_array(list, &XDR.encode_uint/1)

        assert {:ok, ^list, <<>>} =
                 XDR.decode_var_array(encoded, &XDR.decode_uint/1)
      end
    end

    test "variable string array round-trips with alignment padding per element" do
      names = ["alice", "bob", "charlie"]
      encoded = XDR.encode_var_array(names, &XDR.encode_string/1)
      assert {:ok, ^names, <<>>} = XDR.decode_var_array(encoded, &XDR.decode_string/1)
    end
  end

  describe "optional (§4.19)" do
    test "nil encodes as a single FALSE flag" do
      assert XDR.encode_optional(nil, &XDR.encode_int/1) == <<0, 0, 0, 0>>
    end

    test "value encodes as TRUE flag + body" do
      assert XDR.encode_optional(42, &XDR.encode_int/1) ==
               <<0, 0, 0, 1, 0, 0, 0, 42>>
    end

    property "round-trips" do
      check all(
              maybe <-
                one_of([
                  constant(nil),
                  map(integer(-100..100), & &1)
                ])
            ) do
        encoded = XDR.encode_optional(maybe, &XDR.encode_int/1)

        assert {:ok, ^maybe, <<>>} =
                 XDR.decode_optional(encoded, &XDR.decode_int/1)
      end
    end
  end

  describe "discriminated union (§4.15)" do
    test "round-trips with a multi-arm union" do
      # A toy union:
      #   case (int disc) {
      #     case 1: int           // numeric value
      #     case 2: string        // named value
      #     case 3: void          // empty value
      #   }
      encoders = %{
        1 => &XDR.encode_int/1,
        2 => &XDR.encode_string/1,
        3 => &XDR.encode_void/1
      }

      decoders = %{
        1 => &XDR.decode_int/1,
        2 => &XDR.decode_string/1,
        3 => &XDR.decode_void/1
      }

      numeric = XDR.encode_union(1, 99, encoders)
      named = XDR.encode_union(2, "hello", encoders)
      empty = XDR.encode_union(3, :void, encoders)

      assert {:ok, {1, 99}, <<>>} = XDR.decode_union(numeric, decoders)
      assert {:ok, {2, "hello"}, <<>>} = XDR.decode_union(named, decoders)
      assert {:ok, {3, :void}, <<>>} = XDR.decode_union(empty, decoders)
    end

    test "rejects unknown discriminants with :bad_discriminant" do
      decoders = %{1 => &XDR.decode_int/1}

      assert {:error, {:bad_discriminant, 42}} =
               XDR.decode_union(<<0, 0, 0, 42, 0, 0, 0, 0>>, decoders)
    end
  end

  describe "RFC 4506 fixtures" do
    # RFC 4506 §5.1 — a record of {int, uint, bool}.
    test "§5.1: record encodes each primitive sequentially with no padding" do
      encoded = XDR.encode_int(-1) <> XDR.encode_uint(0x7FFFFFFF) <> XDR.encode_bool(true)
      assert byte_size(encoded) == 12

      with {:ok, a, rest} <- XDR.decode_int(encoded),
           {:ok, b, rest} <- XDR.decode_uint(rest),
           {:ok, c, rest} <- XDR.decode_bool(rest) do
        assert a == -1
        assert b == 0x7FFFFFFF
        assert c == true
        assert rest == <<>>
      end
    end

    # RFC 4506 §5.4 — fixed-length array of 2 uints.
    test "§5.4: fixed-length array [3, 4] encodes as 8 bytes" do
      encoded = XDR.encode_fixed_array([3, 4], 2, &XDR.encode_uint/1)
      assert encoded == <<0, 0, 0, 3, 0, 0, 0, 4>>
    end

    # RFC 4506 §5.5 — variable-length array, max 5 elements.
    test "§5.5: variable-length array [1, 2, 3] prefixes count=3" do
      encoded = XDR.encode_var_array([1, 2, 3], &XDR.encode_uint/1)
      assert encoded == <<0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3>>
    end

    # RFC 4506 §5.7 — strings always carry a length prefix and 0-3
    # zero-byte tails.
    test "§5.7: a 7-byte string pads to 12 bytes total" do
      encoded = XDR.encode_string("Elixir!")
      # 4 (length) + 7 (data) + 1 (pad) = 12
      assert byte_size(encoded) == 12
      assert <<0, 0, 0, 7, "Elixir!", 0>> == encoded
    end

    # RFC 4506 §5.8 — optional-data example.
    test "§5.8: a recursive linked-list tail terminates with optional=NULL" do
      # struct list_node { int value; list_node *next; };
      # List: 10 → 20 → NULL
      encoder = fn %{value: v, next: nxt}, encode_next ->
        XDR.encode_int(v) <> XDR.encode_optional(nxt, encode_next)
      end

      # Recursive encoding via a closure captures itself.
      ref = :persistent_term.put({__MODULE__, :tail_encoder}, nil)
      _ = ref

      tail = %{value: 20, next: nil}
      head = %{value: 10, next: tail}

      encode = fn node -> encoder.(node, &encoder.(&1, &1)) end

      head_bytes = encode.(head)
      # 4 (value=10) + 4 (bool=true) + [4 (value=20) + 4 (bool=false)]
      assert byte_size(head_bytes) == 4 + 4 + 4 + 4
    end

    # RFC 4506 §5.11 — void arm.
    test "§5.11: void arm of a discriminated union encodes as disc only (4 bytes)" do
      encoders = %{0 => &XDR.encode_void/1}
      encoded = XDR.encode_union(0, :void, encoders)
      assert encoded == <<0, 0, 0, 0>>
    end
  end
end
