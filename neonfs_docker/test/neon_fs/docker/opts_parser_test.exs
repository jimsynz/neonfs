defmodule NeonFS.Docker.OptsParserTest do
  use ExUnit.Case, async: true

  alias NeonFS.Docker.OptsParser

  describe "parse/1 — happy path" do
    test "empty map yields empty kw list" do
      assert {:ok, []} = OptsParser.parse(%{})
    end

    test "owner passes through verbatim" do
      assert {:ok, [{:owner, "alice"}]} = OptsParser.parse(%{"owner" => "alice"})
    end

    test "atime_mode coerces the string to the matching atom" do
      assert {:ok, [{:atime_mode, :noatime}]} = OptsParser.parse(%{"atime_mode" => "noatime"})
      assert {:ok, [{:atime_mode, :relatime}]} = OptsParser.parse(%{"atime_mode" => "relatime"})
    end

    test "write_ack coerces local/quorum/all" do
      for {wire, expected} <- [{"local", :local}, {"quorum", :quorum}, {"all", :all}] do
        assert {:ok, [{:write_ack, ^expected}]} = OptsParser.parse(%{"write_ack" => wire})
      end
    end

    test "io_weight parses positive integers" do
      assert {:ok, [{:io_weight, 100}]} = OptsParser.parse(%{"io_weight" => "100"})
    end

    test "durability=N expands to a replicate config with majority min_copies" do
      assert {:ok, [{:durability, %{type: :replicate, factor: 1, min_copies: 1}}]} =
               OptsParser.parse(%{"durability" => "1"})

      assert {:ok, [{:durability, %{type: :replicate, factor: 3, min_copies: 2}}]} =
               OptsParser.parse(%{"durability" => "3"})

      assert {:ok, [{:durability, %{type: :replicate, factor: 4, min_copies: 2}}]} =
               OptsParser.parse(%{"durability" => "4"})

      assert {:ok, [{:durability, %{type: :replicate, factor: 5, min_copies: 3}}]} =
               OptsParser.parse(%{"durability" => "5"})
    end

    test "multiple keys produce a kw list of all" do
      {:ok, parsed} = OptsParser.parse(%{"owner" => "bob", "io_weight" => "50"})
      assert Keyword.get(parsed, :owner) == "bob"
      assert Keyword.get(parsed, :io_weight) == 50
    end
  end

  describe "parse/1 — rejection" do
    test "unknown key" do
      assert {:error, msg} = OptsParser.parse(%{"foo" => "bar"})
      assert msg =~ "unknown docker volume opt: foo"
    end

    test "atime_mode with an invalid value" do
      assert {:error, msg} = OptsParser.parse(%{"atime_mode" => "bogus"})
      assert msg =~ "atime_mode"
      assert msg =~ "noatime"
    end

    test "write_ack with an invalid value" do
      assert {:error, msg} = OptsParser.parse(%{"write_ack" => "neither"})
      assert msg =~ "write_ack"
    end

    test "io_weight non-numeric" do
      assert {:error, msg} = OptsParser.parse(%{"io_weight" => "x"})
      assert msg =~ "io_weight must be a positive integer"
    end

    test "io_weight non-positive" do
      assert {:error, msg} = OptsParser.parse(%{"io_weight" => "0"})
      assert msg =~ "io_weight must be a positive integer"
    end

    test "io_weight with trailing garbage" do
      assert {:error, msg} = OptsParser.parse(%{"io_weight" => "100x"})
      assert msg =~ "io_weight must be a positive integer"
    end

    test "durability non-numeric" do
      assert {:error, msg} = OptsParser.parse(%{"durability" => "abc"})
      assert msg =~ "durability"
    end

    test "the first failure halts — later keys aren't reported" do
      # Ordering inside a map isn't guaranteed, but the result is one
      # error string, not a list — confirming halt-on-first-error.
      assert {:error, msg} = OptsParser.parse(%{"foo" => "1", "bar" => "2"})
      assert is_binary(msg)
    end
  end
end
