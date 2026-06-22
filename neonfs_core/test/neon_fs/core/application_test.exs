defmodule NeonFS.Core.ApplicationTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.Application

  describe "distribution_required?/0" do
    setup do
      original = {System.get_env("RELEASE_NAME"), System.get_env("RELEASE_DISTRIBUTION")}

      on_exit(fn ->
        {name, dist} = original
        put_or_delete("RELEASE_NAME", name)
        put_or_delete("RELEASE_DISTRIBUTION", dist)
      end)

      :ok
    end

    test "false outside a release" do
      System.delete_env("RELEASE_NAME")
      refute Application.distribution_required?()
    end

    test "true under a release using named distribution" do
      System.put_env("RELEASE_NAME", "neonfs_core")
      System.put_env("RELEASE_DISTRIBUTION", "name")
      assert Application.distribution_required?()
    end

    test "false when the release explicitly disables distribution" do
      System.put_env("RELEASE_NAME", "neonfs_core")
      System.put_env("RELEASE_DISTRIBUTION", "none")
      refute Application.distribution_required?()
    end
  end

  defp put_or_delete(key, nil), do: System.delete_env(key)
  defp put_or_delete(key, value), do: System.put_env(key, value)
end
