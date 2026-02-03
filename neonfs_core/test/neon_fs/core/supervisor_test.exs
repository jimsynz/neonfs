defmodule NeonFS.Core.SupervisorTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Supervisor, as: CoreSupervisor

  describe "child_specs/0" do
    test "returns specs for all core children" do
      specs = CoreSupervisor.child_specs()

      child_ids =
        Enum.map(specs, fn
          %{id: id} -> id
          {module, _opts} -> module
          module when is_atom(module) -> module
        end)

      assert NeonFS.Core.Persistence in child_ids
      assert NeonFS.Core.BlobStore in child_ids
      assert NeonFS.Core.ChunkIndex in child_ids
      assert NeonFS.Core.FileIndex in child_ids
      assert NeonFS.Core.VolumeRegistry in child_ids
    end

    test "persistence is first in startup order" do
      specs = CoreSupervisor.child_specs()

      first_spec = hd(specs)

      assert match?(%{id: NeonFS.Core.Persistence}, first_spec)
    end
  end

  describe "child_spec_for/1" do
    test "returns spec for known module" do
      spec = CoreSupervisor.child_spec_for(NeonFS.Core.BlobStore)
      assert spec != nil
    end

    test "returns nil for unknown module" do
      spec = CoreSupervisor.child_spec_for(NonExistent.Module)
      assert spec == nil
    end
  end
end
