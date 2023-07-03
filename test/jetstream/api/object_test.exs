defmodule Jetstream.API.ObjectTest do
  use Jetstream.ConnCase, min_server_version: "2.6.2"
  alias Jetstream.API.Object

  @moduletag with_gnat: :gnat
  @readme_path Path.join([Path.dirname(__DIR__), "..", "..", "README.md"])

  describe "create_bucket/3" do
    test "create/delete a bucket" do
      assert {:ok, %{config: config}} = Object.create_bucket(:gnat, "MY-STORE")
      assert config.name == "OBJ_MY-STORE"
      assert config.max_age == 0
      assert config.max_bytes == -1
      assert config.storage == :file
      assert config.allow_rollup_hdrs == true

      assert config.subjects == [
               "$O.MY-STORE.C.>",
               "$O.MY-STORE.M.>"
             ]

      assert :ok = Object.delete_bucket(:gnat, "MY-STORE")
    end

    test "bucket names are validated" do
      assert {:error, "invalid bucket name"} = Object.create_bucket(:gnat, "")
      assert {:error, "invalid bucket name"} = Object.create_bucket(:gnat, "MY.STORE")
      assert {:error, "invalid bucket name"} = Object.create_bucket(:gnat, "(*!&@($%*&))")
    end
  end

  describe "delete_bucket/2" do
    test "create/delete a bucket" do
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, "MY-STORE")
      assert :ok = Object.delete_bucket(:gnat, "MY-STORE")
    end
  end

  describe "list_objects/3" do
    test "list an empty bucket" do
      nuid = Jetstream.API.Util.nuid()
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, nuid)
      assert {:ok, []} = Object.list_objects(:gnat, nuid)
    end

    test "list a bucket with two files" do
      nuid = Jetstream.API.Util.nuid()
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, nuid)
      assert {:ok, io} = File.open(@readme_path, [:read])
      assert {:ok, _object} = Object.put_object(:gnat, nuid, "README.md", io)
      assert {:ok, io} = File.open(@readme_path, [:read])
      assert {:ok, _object} = Object.put_object(:gnat, nuid, "SOMETHING.md", io)

      assert {:ok, objects} = Object.list_objects(:gnat, nuid)
      [readme, something] = Enum.sort_by(objects, & &1.name)
      assert readme.name == "README.md"
      assert readme.size == something.size
      assert readme.digest == something.digest
    end
  end

  describe "put_object/4" do
    test "creates an object" do
      {:ok, bytes} = File.read(@readme_path)
      sha = :crypto.hash(:sha256, bytes)
      assert {:ok, io} = File.open(@readme_path, [:read])

      assert {:ok, %{config: _stream}} = Object.create_bucket(:gnat, "MY-STORE")
      assert {:ok, object_meta} = Object.put_object(:gnat, "MY-STORE", "README.md", io)
      assert object_meta.name == "README.md"
      assert object_meta.bucket == "MY-STORE"
      assert object_meta.chunks == 1
      assert "SHA-256=" <> encoded = object_meta.digest
      assert Base.decode64!(encoded) == sha
      assert :ok = Object.delete_bucket(:gnat, "MY-STORE")
    end

    test "return an error if the object store doesn't exist" do
      assert {:ok, io} = File.open(@readme_path, [:read])
      assert {:error, err} = Object.put_object(:gnat, "I_DONT_EXIST", "foo", io)
      assert %{"code" => 404, "description" => "stream not found"} = err
    end
  end
end
