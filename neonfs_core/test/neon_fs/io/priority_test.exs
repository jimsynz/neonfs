defmodule NeonFS.IO.PriorityTest do
  use ExUnit.Case, async: true

  alias NeonFS.IO.Priority

  test ":metadata_commit is a valid class weighted below user_read and above user_write (#1305)" do
    assert Priority.valid?(:metadata_commit)
    assert :metadata_commit in Priority.all()

    assert Priority.weight(:user_read) > Priority.weight(:metadata_commit)
    assert Priority.weight(:metadata_commit) > Priority.weight(:user_write)
  end

  test "all/0 weights are strictly descending in declared order" do
    weights = Enum.map(Priority.all(), &Priority.weight/1)
    assert weights == Enum.sort(weights, :desc)
    assert weights == Enum.uniq(weights)
  end
end
