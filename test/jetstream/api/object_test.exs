defmodule Jetstream.API.ObjectTest do
  use Jetstream.ConnCase, min_server_version: "2.6.2"
  alias Jetstream.API.Object
  import Jetstream.API.Util, only: [nuid: 0]

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

  describe "get_object/4" do
    test "retrieves and object chunk-by-chunk" do
      nuid = Jetstream.API.Util.nuid()
      assert {:ok, _} = Object.create_bucket(:gnat, nuid)
      readme_content = File.read!(@readme_path)
      assert {:ok, _meta} = put_filepath(@readme_path, nuid, "README.md")

      Object.get_object(:gnat, nuid, "README.md", fn chunk ->
        assert chunk == readme_content
        send(self(), :got_chunk)
      end)

      assert_received :got_chunk

      Object.delete_bucket(:gnat, nuid)
    end
  end

  describe "info/3" do
    test "lookup meta information about an object" do
      assert {:ok, %{config: _stream}} = Object.create_bucket(:gnat, "INF")
      assert {:ok, io} = File.open(@readme_path, [:read])
      assert {:ok, initial_meta} = Object.put_object(:gnat, "INF", "README.md", io)

      assert {:ok, lookup_meta} = Object.info(:gnat, "INF", "README.md")
      assert lookup_meta == initial_meta

      assert :ok = Object.delete_bucket(:gnat, "INF")
    end
  end

  describe "list_objects/3" do
    test "list an empty bucket" do
      bucket = nuid()
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, bucket)
      assert {:ok, []} = Object.list_objects(:gnat, bucket)
    end

    test "list a bucket with two files" do
      bucket = nuid()
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, bucket)
      assert {:ok, io} = File.open(@readme_path, [:read])
      assert {:ok, _object} = Object.put_object(:gnat, bucket, "README.md", io)
      assert {:ok, io} = File.open(@readme_path, [:read])
      assert {:ok, _object} = Object.put_object(:gnat, bucket, "SOMETHING.md", io)

      assert {:ok, objects} = Object.list_objects(:gnat, bucket)
      [readme, something] = Enum.sort_by(objects, & &1.name)
      assert readme.name == "README.md"
      assert readme.size == something.size
      assert readme.digest == something.digest
    end
  end

  describe "put_object/4" do
    test "creates an object" do
      assert {:ok, %{config: _stream}} = Object.create_bucket(:gnat, "MY-STORE")

      expected_sha = @readme_path |> File.read!() |> then(&:crypto.hash(:sha256, &1))
      assert {:ok, object_meta} = put_filepath(@readme_path, "MY-STORE", "README.md")
      assert object_meta.name == "README.md"
      assert object_meta.bucket == "MY-STORE"
      assert object_meta.chunks == 1
      assert "SHA-256=" <> encoded = object_meta.digest
      assert Base.decode64!(encoded) == expected_sha

      assert :ok = Object.delete_bucket(:gnat, "MY-STORE")
    end

    test "return an error if the object store doesn't exist" do
      assert {:error, err} = put_filepath(@readme_path, "I_DONT_EXIST", "foo")
      assert %{"code" => 404, "description" => "stream not found"} = err
    end
  end

  defp put_filepath(path, bucket, name) do
    {:ok, io} = File.open(path, [:read])
    Object.put_object(:gnat, bucket, name, io)
  end
end