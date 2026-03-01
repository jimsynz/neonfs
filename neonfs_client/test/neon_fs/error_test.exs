defmodule NeonFS.ErrorTest do
  use ExUnit.Case, async: true

  alias NeonFS.Error

  describe "error class creation and rendering" do
    test "Invalid class with message" do
      error = %Error.Invalid{message: "bad input"}
      assert Exception.message(error) =~ "bad input"
    end

    test "Invalid class default message" do
      error = %Error.Invalid{}
      assert Exception.message(error) =~ "Invalid input"
    end

    test "NotFound class with message" do
      error = %Error.NotFound{message: "gone"}
      assert Exception.message(error) =~ "gone"
    end

    test "NotFound class default message" do
      error = %Error.NotFound{}
      assert Exception.message(error) =~ "Resource not found"
    end

    test "Forbidden class with message" do
      error = %Error.Forbidden{message: "no access"}
      assert Exception.message(error) =~ "no access"
    end

    test "Forbidden class default message" do
      error = %Error.Forbidden{}
      assert Exception.message(error) =~ "Permission denied"
    end

    test "Unavailable class with message" do
      error = %Error.Unavailable{message: "cluster down"}
      assert Exception.message(error) =~ "cluster down"
    end

    test "Unavailable class default message" do
      error = %Error.Unavailable{}
      assert Exception.message(error) =~ "Service unavailable"
    end

    test "Internal class with message" do
      error = %Error.Internal{message: "NIF crashed"}
      assert Exception.message(error) =~ "NIF crashed"
    end

    test "Internal class default message" do
      error = %Error.Internal{}
      assert Exception.message(error) =~ "Internal error"
    end

    test "error classes support details field" do
      error = %Error.Invalid{message: "bad", details: %{field: :name}}
      assert error.details == %{field: :name}
    end
  end

  describe "specific error modules" do
    test "VolumeNotFound with volume_name" do
      error = %Error.VolumeNotFound{volume_name: "data"}
      assert Exception.message(error) == "Volume 'data' not found"
    end

    test "VolumeNotFound with volume_id" do
      error = %Error.VolumeNotFound{volume_id: "vol_123"}
      assert Exception.message(error) == "Volume with ID 'vol_123' not found"
    end

    test "VolumeNotFound default message" do
      error = %Error.VolumeNotFound{}
      assert Exception.message(error) == "Volume not found"
    end

    test "FileNotFound with file_path" do
      error = %Error.FileNotFound{file_path: "/data/readme.txt"}
      assert Exception.message(error) == "File not found: /data/readme.txt"
    end

    test "FileNotFound default message" do
      error = %Error.FileNotFound{}
      assert Exception.message(error) == "File not found"
    end

    test "ChunkNotFound with chunk_hash" do
      error = %Error.ChunkNotFound{chunk_hash: "abc123"}
      assert Exception.message(error) == "Chunk not found: abc123"
    end

    test "ChunkNotFound default message" do
      error = %Error.ChunkNotFound{}
      assert Exception.message(error) == "Chunk not found"
    end

    test "QuorumUnavailable with full context" do
      error = %Error.QuorumUnavailable{operation: "write", required: 2, available: 1}
      assert Exception.message(error) == "Quorum unavailable for write: need 2, have 1"
    end

    test "QuorumUnavailable with operation only" do
      error = %Error.QuorumUnavailable{operation: "read"}
      assert Exception.message(error) == "Quorum unavailable for read"
    end

    test "QuorumUnavailable default message" do
      error = %Error.QuorumUnavailable{}
      assert Exception.message(error) == "Quorum unavailable"
    end

    test "PermissionDenied with path and operation" do
      error = %Error.PermissionDenied{file_path: "/secret", operation: "read"}
      assert Exception.message(error) == "Permission denied: read on /secret"
    end

    test "PermissionDenied with path only" do
      error = %Error.PermissionDenied{file_path: "/secret"}
      assert Exception.message(error) == "Permission denied: /secret"
    end

    test "PermissionDenied default message" do
      error = %Error.PermissionDenied{}
      assert Exception.message(error) == "Permission denied"
    end

    test "InvalidPath with path and reason" do
      error = %Error.InvalidPath{file_path: "../escape", reason: "relative paths not allowed"}
      assert Exception.message(error) == "Invalid path '../escape': relative paths not allowed"
    end

    test "InvalidPath with path only" do
      error = %Error.InvalidPath{file_path: ""}
      assert Exception.message(error) == "Invalid path: "
    end

    test "InvalidPath default message" do
      error = %Error.InvalidPath{}
      assert Exception.message(error) == "Invalid path"
    end

    test "InvalidConfig with field and reason" do
      error = %Error.InvalidConfig{field: :chunk_size, reason: "must be positive"}
      assert Exception.message(error) == "Invalid configuration for chunk_size: must be positive"
    end

    test "InvalidConfig with field only" do
      error = %Error.InvalidConfig{field: :chunk_size}
      assert Exception.message(error) == "Invalid configuration: chunk_size"
    end

    test "InvalidConfig default message" do
      error = %Error.InvalidConfig{}
      assert Exception.message(error) == "Invalid configuration"
    end
  end

  describe "to_string/1" do
    test "renders error class message" do
      assert Error.to_string(%Error.Internal{message: "boom"}) =~ "boom"
    end

    test "renders specific error message" do
      error = %Error.VolumeNotFound{volume_name: "data"}
      assert Error.to_string(error) == "Volume 'data' not found"
    end

    test "handles non-exception values" do
      result = Error.to_string(:not_an_error)
      assert is_binary(result)
    end
  end

  describe "String.Chars protocol" do
    test "error class supports interpolation" do
      error = %Error.Internal{message: "NIF crash"}
      assert "#{error}" =~ "NIF crash"
    end

    test "specific error supports interpolation" do
      error = %Error.VolumeNotFound{volume_name: "media"}
      assert "#{error}" == "Volume 'media' not found"
    end

    test "all error classes implement String.Chars" do
      assert "#{%Error.Invalid{message: "bad"}}" =~ "bad"
      assert "#{%Error.NotFound{message: "gone"}}" =~ "gone"
      assert "#{%Error.Forbidden{message: "no"}}" =~ "no"
      assert "#{%Error.Unavailable{message: "down"}}" =~ "down"
      assert "#{%Error.Internal{message: "oops"}}" =~ "oops"
    end

    test "all specific errors implement String.Chars" do
      assert is_binary("#{%Error.VolumeNotFound{}}")
      assert is_binary("#{%Error.FileNotFound{}}")
      assert is_binary("#{%Error.ChunkNotFound{}}")
      assert is_binary("#{%Error.QuorumUnavailable{}}")
      assert is_binary("#{%Error.PermissionDenied{}}")
      assert is_binary("#{%Error.InvalidPath{}}")
      assert is_binary("#{%Error.InvalidConfig{}}")
      assert is_binary("#{%Error.Unknown{}}")
    end
  end

  describe "errors in tuples" do
    test "error structs work in {:error, struct} tuples" do
      result = {:error, %Error.VolumeNotFound{volume_name: "data"}}
      assert {:error, %Error.VolumeNotFound{volume_name: "data"}} = result
    end

    test "pattern matching on error class" do
      result = {:error, %Error.VolumeNotFound{volume_name: "data"}}

      message =
        case result do
          {:error, %{class: :not_found} = error} -> Exception.message(error)
          _ -> "other"
        end

      assert message == "Volume 'data' not found"
    end

    test "error class field is set correctly" do
      assert %Error.Invalid{}.class == :invalid
      assert %Error.NotFound{}.class == :not_found
      assert %Error.Forbidden{}.class == :forbidden
      assert %Error.Unavailable{}.class == :unavailable
      assert %Error.Internal{}.class == :internal
      assert %Error.VolumeNotFound{}.class == :not_found
      assert %Error.PermissionDenied{}.class == :forbidden
      assert %Error.QuorumUnavailable{}.class == :unavailable
      assert %Error.InvalidPath{}.class == :invalid
      assert %Error.InvalidConfig{}.class == :invalid
    end
  end
end
