defmodule Jetstream.API.ObjectTest do
  use Jetstream.ConnCase, min_server_version: "2.6.2"
  alias Jetstream.API.{Object, Stream}
  import Jetstream.API.Util, only: [nuid: 0]

  @moduletag with_gnat: :gnat
  @changelog_path Path.join([Path.dirname(__DIR__), "..", "..", "CHANGELOG.md"])
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

    test "creating a bucket with TTL" do
      bucket = nuid()
      ttl = 10 * 1_000_000_000 # 10s in nanoseconds
      assert {:ok, %{config: config}} = Object.create_bucket(:gnat, bucket, ttl: ttl)
      assert config.max_age == ttl

      assert :ok = Object.delete_bucket(:gnat, bucket)
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

  describe "delete_object/3" do
    test "delete an object" do
      bucket = nuid()
      assert {:ok, %{config: _config}} = Object.create_bucket(:gnat, bucket)
      {:ok, _} = put_filepath(@readme_path, bucket, "README.md")
      {:ok, _} = put_filepath(@readme_path, bucket, "OTHER.md")
      assert :ok = Object.delete_object(:gnat, bucket, "README.md")

      assert {:ok, objects} = Object.list_objects(:gnat, bucket)
      assert Enum.count(objects) == 1
      assert Enum.map(objects, & &1.name) == ["OTHER.md"]
      assert {:ok, objects} = Object.list_objects(:gnat, bucket, show_deleted: true)
      assert Enum.count(objects) == 2
      assert Enum.map(objects, & &1.name) |> Enum.sort() == ["OTHER.md", "README.md"]

      assert :ok = Object.delete_bucket(:gnat, bucket)
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

      :ok = Object.delete_bucket(:gnat, nuid)
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
      assert :ok = Object.delete_bucket(:gnat, bucket)
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

      assert :ok = Object.delete_bucket(:gnat, bucket)
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
      assert Base.url_decode64!(encoded) == expected_sha

      assert :ok = Object.delete_bucket(:gnat, "MY-STORE")
    end

    test "overwriting a file" do
      bucket = nuid()
      assert {:ok, %{config: _stream}} = Object.create_bucket(:gnat, bucket)
      assert {:ok, _} = put_filepath(@readme_path, bucket, "WAT")
      size_after_readme = stream_byte_size(bucket)
      assert {:ok, _} = put_filepath(@changelog_path, bucket, "WAT")
      size_after_changelog = stream_byte_size(bucket)
      assert size_after_changelog < size_after_readme
      assert {:ok, [meta]} = Object.list_objects(:gnat, bucket)
      assert meta.name == "WAT"
    end

    test "return an error if the object store doesn't exist" do
      assert {:error, err} = put_filepath(@readme_path, "I_DONT_EXIST", "foo")
      assert %{"code" => 404, "description" => "stream not found"} = err
    end
  end

  test "storing and retrieving larger files" do
    assert {:ok, path, sha} = generate_big_file()
    bucket = nuid()
    assert {:ok, %{config: _stream}} = Object.create_bucket(:gnat, bucket)
    assert {:ok, meta} = put_filepath(path, bucket, "big")
    assert meta.chunks == 16
    assert meta.size == 16 * 128 * 1024
    assert "SHA-256=" <> encoded = meta.digest
    assert Base.url_decode64!(encoded) == sha

    Process.put(:buffer, "")

    Object.get_object(:gnat, bucket, "big", fn chunk ->
      Process.put(:buffer, Process.get(:buffer) <> chunk)
    end)

    file_contents = Process.get(:buffer)
    assert byte_size(file_contents) == meta.size
    assert :crypto.hash(:sha256, file_contents) == sha
    assert stream_byte_size(bucket) > 2 * 1024 * 1024

    assert :ok = Object.delete_object(:gnat, bucket, "big")
    assert stream_byte_size(bucket) < 1024
    :ok = Object.delete_bucket(:gnat, bucket)
  end

  # create a random 2MB binary file
  # re-use it on subsequent test runs if it already exists
  defp generate_big_file do
    filepath = Path.join("tmp", "big_file.bin")

    if File.exists?(filepath) do
      content = File.read!(filepath)
      sha = :crypto.hash(:sha256, content)
      {:ok, filepath, sha}
    else
      {:ok, _} =
        File.open(filepath, [:write], fn fh ->
          Enum.each(1..16, fn _ ->
            rand_chunk = :crypto.strong_rand_bytes(128) |> String.duplicate(1024)

            :ok = IO.binwrite(fh, rand_chunk)
          end)
        end)

      generate_big_file()
    end
  end

  defp put_filepath(path, bucket, name) do
    {:ok, io} = File.open(path, [:read])
    Object.put_object(:gnat, bucket, name, io)
  end

  defp stream_byte_size(bucket) do
    {:ok, %{state: state}} = Stream.info(:gnat, "OBJ_#{bucket}")
    state.bytes
  end
end
