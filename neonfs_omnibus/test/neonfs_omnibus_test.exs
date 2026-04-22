defmodule NeonFS.Omnibus.ApplicationTest do
  use ExUnit.Case

  alias NeonFS.Omnibus.MixProject

  test "omnibus module is defined" do
    assert Code.ensure_loaded?(NeonFS.Omnibus)
  end

  describe "bundled service modules are loadable" do
    test "NeonFS.Core" do
      assert Code.ensure_loaded?(NeonFS.Core)
    end

    test "NeonFS.FUSE" do
      assert Code.ensure_loaded?(NeonFS.FUSE)
    end

    test "NeonFS.NFS" do
      assert Code.ensure_loaded?(NeonFS.NFS)
    end

    test "NeonFS.S3" do
      assert Code.ensure_loaded?(NeonFS.S3)
    end

    test "NeonFS.WebDAV" do
      assert Code.ensure_loaded?(NeonFS.WebDAV)
    end

    test "NeonFS.Docker" do
      assert Code.ensure_loaded?(NeonFS.Docker)
    end
  end

  describe "release configuration" do
    test "includes webdav as a loaded application" do
      releases = MixProject.project()[:releases]
      apps = releases[:neonfs_omnibus][:applications]

      assert apps[:neonfs_webdav] == :load
    end

    test "includes all service applications" do
      releases = MixProject.project()[:releases]
      apps = releases[:neonfs_omnibus][:applications]

      assert apps[:neonfs_fuse] == :load
      assert apps[:neonfs_nfs] == :load
      assert apps[:neonfs_s3] == :load
      assert apps[:neonfs_webdav] == :load
      assert apps[:neonfs_docker] == :load
    end
  end
end
