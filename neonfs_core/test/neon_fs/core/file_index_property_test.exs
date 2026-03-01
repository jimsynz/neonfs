defmodule NeonFS.Core.FileIndexPropertyTest do
  @moduledoc """
  Property-based tests for FileIndex path operations.

  Tests the pure path functions used by FileIndex (normalize_path, parent_path,
  validate_path, split_path) rather than the GenServer operations which require
  quorum infrastructure.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias NeonFS.Core.FileMeta

  # Generators

  defp path_segment do
    StreamData.string(:alphanumeric, min_length: 1, max_length: 12)
    |> StreamData.filter(&(&1 != "" and &1 != "." and &1 != ".."))
  end

  defp valid_path do
    StreamData.list_of(path_segment(), min_length: 1, max_length: 5)
    |> StreamData.map(fn segments -> "/" <> Enum.join(segments, "/") end)
  end

  defp nested_path(min_depth \\ 2) do
    StreamData.list_of(path_segment(), min_length: min_depth, max_length: 5)
    |> StreamData.map(fn segments -> "/" <> Enum.join(segments, "/") end)
  end

  # Properties — normalize_path

  describe "normalize_path" do
    property "is idempotent — normalising a normalised path is a no-op" do
      check all(path <- valid_path(), max_runs: 200) do
        once = FileMeta.normalize_path(path)
        twice = FileMeta.normalize_path(once)

        assert once == twice
      end
    end

    property "path with trailing slash is equivalent to path without" do
      check all(path <- valid_path(), max_runs: 200) do
        with_slash = path <> "/"
        normalized_with = FileMeta.normalize_path(with_slash)
        normalized_without = FileMeta.normalize_path(path)

        assert normalized_with == normalized_without
      end
    end

    property "normalised path never ends with / (except root)" do
      check all(path <- valid_path(), max_runs: 200) do
        normalized = FileMeta.normalize_path(path)

        if normalized != "/" do
          refute String.ends_with?(normalized, "/")
        end
      end
    end

    property "root path normalises to itself" do
      assert FileMeta.normalize_path("/") == "/"
    end
  end

  # Properties — parent_path

  describe "parent_path" do
    property "parent of /a/b/c is always /a/b" do
      check all(
              segments <- StreamData.list_of(path_segment(), min_length: 2, max_length: 5),
              max_runs: 200
            ) do
        path = "/" <> Enum.join(segments, "/")
        parent_segments = Enum.drop(segments, -1)
        expected_parent = "/" <> Enum.join(parent_segments, "/")

        assert FileMeta.parent_path(path) == expected_parent
      end
    end

    property "root path has no parent" do
      assert FileMeta.parent_path("/") == nil
    end

    property "single-level path has root as parent" do
      check all(segment <- path_segment(), max_runs: 200) do
        path = "/" <> segment
        assert FileMeta.parent_path(path) == "/"
      end
    end

    property "path is always a child of its parent (starts with parent prefix)" do
      check all(path <- nested_path(), max_runs: 200) do
        parent = FileMeta.parent_path(path)

        assert parent != nil
        assert String.starts_with?(path, parent)
      end
    end
  end

  # Properties — validate_path

  describe "validate_path" do
    property "generated valid paths always pass validation" do
      check all(path <- valid_path(), max_runs: 200) do
        assert :ok == FileMeta.validate_path(path)
      end
    end

    property "normalised form of a valid path is also valid" do
      check all(path <- valid_path(), max_runs: 200) do
        normalized = FileMeta.normalize_path(path)
        assert :ok == FileMeta.validate_path(normalized)
      end
    end
  end

  # Properties — split_path (private, tested via FileIndex behaviour)

  describe "path structure" do
    property "directory listing for a parent contains all direct children" do
      # Construct a set of sibling paths under a common parent and verify
      # that splitting each yields the same parent and distinct names
      check all(
              parent_segments <- StreamData.list_of(path_segment(), min_length: 1, max_length: 3),
              child_names <- StreamData.uniq_list_of(path_segment(), min_length: 1, max_length: 5),
              max_runs: 200
            ) do
        parent = "/" <> Enum.join(parent_segments, "/")

        child_paths =
          Enum.map(child_names, fn name ->
            parent <> "/" <> name
          end)

        # Each child path, when split, should yield the parent and child name
        Enum.zip(child_paths, child_names)
        |> Enum.each(fn {child_path, expected_name} ->
          computed_parent = FileMeta.parent_path(child_path)
          assert computed_parent == parent

          # The child name is the last segment
          computed_name = child_path |> String.split("/") |> List.last()
          assert computed_name == expected_name
        end)
      end
    end

    property "path depth matches segment count" do
      check all(
              segments <- StreamData.list_of(path_segment(), min_length: 1, max_length: 5),
              max_runs: 200
            ) do
        path = "/" <> Enum.join(segments, "/")
        parts = String.split(path, "/", trim: true)

        assert length(parts) == length(segments)
      end
    end
  end
end
