defmodule NeonFS.Core.DriveConfigTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.DriveConfig

  @mib 1_048_576
  @gib 1_073_741_824
  @tib 1_099_511_627_776

  describe "parse_capacity/1" do
    test "parses raw integer strings" do
      assert {:ok, 0} = DriveConfig.parse_capacity("0")
      assert {:ok, 1_000_000} = DriveConfig.parse_capacity("1000000")
      assert {:ok, 1_099_511_627_776} = DriveConfig.parse_capacity("1099511627776")
    end

    test "parses M suffix (mebibytes)" do
      assert DriveConfig.parse_capacity("100M") == {:ok, 100 * @mib}
      assert DriveConfig.parse_capacity("100m") == {:ok, 100 * @mib}
      assert DriveConfig.parse_capacity("512M") == {:ok, 512 * @mib}
    end

    test "parses G suffix (gibibytes)" do
      assert DriveConfig.parse_capacity("1G") == {:ok, @gib}
      assert DriveConfig.parse_capacity("1g") == {:ok, @gib}
      assert DriveConfig.parse_capacity("500G") == {:ok, 500 * @gib}
    end

    test "parses T suffix (tebibytes)" do
      assert DriveConfig.parse_capacity("1T") == {:ok, @tib}
      assert DriveConfig.parse_capacity("1t") == {:ok, @tib}
      assert DriveConfig.parse_capacity("2T") == {:ok, 2 * @tib}
      assert DriveConfig.parse_capacity("4T") == {:ok, 4 * @tib}
    end

    test "parses float values with suffix" do
      assert DriveConfig.parse_capacity("1.5G") == {:ok, trunc(1.5 * @gib)}
      assert DriveConfig.parse_capacity("0.5T") == {:ok, trunc(0.5 * @tib)}
      assert DriveConfig.parse_capacity("2.5M") == {:ok, trunc(2.5 * @mib)}
    end

    test "handles whitespace" do
      assert DriveConfig.parse_capacity("  1G  ") == {:ok, @gib}
    end

    test "returns error for invalid formats" do
      assert {:error, _} = DriveConfig.parse_capacity("")
      assert {:error, _} = DriveConfig.parse_capacity("abc")
      assert {:error, _} = DriveConfig.parse_capacity("1X")
      assert {:error, _} = DriveConfig.parse_capacity("G")
      assert {:error, _} = DriveConfig.parse_capacity("-1G")
    end
  end

  describe "parse_capacity!/1" do
    test "returns bytes on valid input" do
      assert DriveConfig.parse_capacity!("1G") == @gib
      assert DriveConfig.parse_capacity!("0") == 0
    end

    test "raises ArgumentError on invalid input" do
      assert_raise ArgumentError, fn ->
        DriveConfig.parse_capacity!("invalid")
      end
    end
  end

  describe "detect_capacity/1" do
    test "no-op when capacity is already set" do
      drive = %NeonFS.Core.Drive{
        id: "test",
        node: Node.self(),
        path: "/tmp",
        tier: :hot,
        capacity_bytes: 1_000_000
      }

      assert DriveConfig.detect_capacity(drive).capacity_bytes == 1_000_000
    end

    test "fills capacity from filesystem when capacity is zero" do
      drive = %NeonFS.Core.Drive{
        id: "test",
        node: Node.self(),
        path: "/tmp",
        tier: :hot,
        capacity_bytes: 0
      }

      result = DriveConfig.detect_capacity(drive)
      assert result.capacity_bytes > 0
    end

    test "returns drive unchanged on nonexistent path" do
      drive = %NeonFS.Core.Drive{
        id: "test",
        node: Node.self(),
        path: "/nonexistent/path/that/does/not/exist",
        tier: :hot,
        capacity_bytes: 0
      }

      assert DriveConfig.detect_capacity(drive).capacity_bytes == 0
    end
  end

  describe "validate_drives/1" do
    test "skips drives with zero capacity" do
      drive = %NeonFS.Core.Drive{
        id: "test",
        node: Node.self(),
        path: "/tmp",
        tier: :hot,
        capacity_bytes: 0
      }

      assert :ok = DriveConfig.validate_drives([drive])
    end

    test "validates drive against actual filesystem" do
      # Use /tmp which always exists — this should not warn since we
      # set capacity to 1 byte (less than any real partition)
      drive = %NeonFS.Core.Drive{
        id: "test",
        node: Node.self(),
        path: "/tmp",
        tier: :hot,
        capacity_bytes: 1
      }

      assert :ok = DriveConfig.validate_drives([drive])
    end

    test "handles nonexistent paths gracefully" do
      drive = %NeonFS.Core.Drive{
        id: "test",
        node: Node.self(),
        path: "/nonexistent/path/that/does/not/exist",
        tier: :hot,
        capacity_bytes: 1_000_000
      }

      # Should not raise — logs a debug message and continues
      assert :ok = DriveConfig.validate_drives([drive])
    end
  end
end
