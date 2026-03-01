defmodule NeonFS.IO.OperationTest do
  use ExUnit.Case, async: true

  alias NeonFS.IO.{Operation, Priority}

  describe "new/1" do
    test "creates an operation with all required fields" do
      op =
        Operation.new(
          priority: :user_read,
          volume_id: "vol-1",
          drive_id: "nvme0",
          type: :read,
          callback: fn -> :ok end
        )

      assert is_binary(op.id)
      assert byte_size(op.id) == 16
      assert op.priority == :user_read
      assert op.volume_id == "vol-1"
      assert op.drive_id == "nvme0"
      assert op.type == :read
      assert is_function(op.callback, 0)
      assert is_integer(op.submitted_at)
      assert op.metadata == %{}
    end

    test "creates operations with each priority class" do
      for priority <- Priority.all() do
        op =
          Operation.new(
            priority: priority,
            volume_id: "vol-1",
            drive_id: "sda",
            type: :write,
            callback: fn -> :ok end
          )

        assert op.priority == priority
      end
    end

    test "generates unique IDs" do
      ops =
        for _ <- 1..100 do
          Operation.new(
            priority: :user_read,
            volume_id: "vol-1",
            drive_id: "nvme0",
            type: :read,
            callback: fn -> :ok end
          )
        end

      ids = Enum.map(ops, & &1.id)
      assert length(Enum.uniq(ids)) == 100
    end

    test "accepts custom metadata" do
      op =
        Operation.new(
          priority: :replication,
          volume_id: "vol-1",
          drive_id: "sda",
          type: :write,
          callback: fn -> :ok end,
          metadata: %{chunk_hash: "abc123"}
        )

      assert op.metadata == %{chunk_hash: "abc123"}
    end

    test "allows overriding the ID" do
      op =
        Operation.new(
          id: "custom-id",
          priority: :user_read,
          volume_id: "vol-1",
          drive_id: "nvme0",
          type: :read,
          callback: fn -> :ok end
        )

      assert op.id == "custom-id"
    end
  end

  describe "validate/1" do
    test "succeeds for a valid operation" do
      op =
        Operation.new(
          priority: :user_write,
          volume_id: "vol-1",
          drive_id: "nvme0",
          type: :write,
          callback: fn -> :ok end
        )

      assert {:ok, ^op} = Operation.validate(op)
    end

    test "fails with invalid priority" do
      op = %Operation{
        id: "test",
        priority: :invalid,
        volume_id: "vol-1",
        drive_id: "nvme0",
        type: :read,
        callback: fn -> :ok end,
        submitted_at: 0
      }

      assert {:error, "invalid priority: :invalid"} = Operation.validate(op)
    end

    test "fails with empty volume_id" do
      op = %Operation{
        id: "test",
        priority: :user_read,
        volume_id: "",
        drive_id: "nvme0",
        type: :read,
        callback: fn -> :ok end,
        submitted_at: 0
      }

      assert {:error, "volume_id must be a non-empty string"} = Operation.validate(op)
    end

    test "fails with nil drive_id" do
      op = %Operation{
        id: "test",
        priority: :user_read,
        volume_id: "vol-1",
        drive_id: nil,
        type: :read,
        callback: fn -> :ok end,
        submitted_at: 0
      }

      assert {:error, "drive_id must be a non-empty string"} = Operation.validate(op)
    end

    test "fails with invalid type" do
      op = %Operation{
        id: "test",
        priority: :user_read,
        volume_id: "vol-1",
        drive_id: "nvme0",
        type: :delete,
        callback: fn -> :ok end,
        submitted_at: 0
      }

      assert {:error, "type must be :read or :write, got: :delete"} = Operation.validate(op)
    end

    test "fails with non-zero-arity callback" do
      op = %Operation{
        id: "test",
        priority: :user_read,
        volume_id: "vol-1",
        drive_id: "nvme0",
        type: :read,
        callback: fn _x -> :ok end,
        submitted_at: 0
      }

      assert {:error, "callback must be a zero-arity function"} = Operation.validate(op)
    end

    test "fails with nil id" do
      op = %Operation{
        id: nil,
        priority: :user_read,
        volume_id: "vol-1",
        drive_id: "nvme0",
        type: :read,
        callback: fn -> :ok end,
        submitted_at: 0
      }

      assert {:error, "id must be a non-empty string"} = Operation.validate(op)
    end
  end
end
