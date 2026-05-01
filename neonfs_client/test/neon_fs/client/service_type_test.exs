defmodule NeonFS.Client.ServiceTypeTest do
  use ExUnit.Case, async: true

  alias NeonFS.Client.ServiceType
  import NeonFS.Client.ServiceType, only: [is_service_type: 1]

  describe "all/0" do
    test "returns all service types in alphabetical order" do
      assert ServiceType.all() ==
               [:cifs, :containerd, :core, :csi, :docker, :fuse, :nfs, :s3, :webdav]
    end
  end

  describe "core?/1" do
    test "returns true for :core" do
      assert ServiceType.core?(:core) == true
    end

    test "returns false for non-core types" do
      for type <- [:fuse, :nfs, :s3, :webdav, :docker, :csi, :cifs, :containerd] do
        assert ServiceType.core?(type) == false
      end
    end

    test "returns false for invalid types" do
      assert ServiceType.core?(:bogus) == false
      assert ServiceType.core?(nil) == false
    end
  end

  describe "is_service_type/1 guard" do
    test "matches valid service types" do
      for type <- ServiceType.all() do
        assert is_service_type(type)
      end
    end

    test "does not match invalid values" do
      refute is_service_type(:bogus)
      refute is_service_type("core")
      refute is_service_type(nil)
    end
  end
end
