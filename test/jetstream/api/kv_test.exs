defmodule Jetstream.API.KVTest do
  use Jetstream.ConnCase, min_server_version: "2.6.2"
  alias Jetstream.API.KV

  @moduletag with_gnat: :gnat

  describe "create_bucket/3" do
    test "creates a bucket" do
      assert {:ok, %{config: config}} = KV.create_bucket(:gnat, "BUCKET_TEST")
      assert config.name == "KV_BUCKET_TEST"
      assert config.subjects == ["$KV.BUCKET_TEST.>"]
      assert config.max_msgs_per_subject == 1
      assert config.discard == :new
      assert config.allow_rollup_hdrs == true

      assert :ok = KV.delete_bucket(:gnat, "BUCKET_TEST")
    end

    test "creates a bucket with duplicate window < 2min" do
      assert {:ok, %{config: config}} = KV.create_bucket(:gnat, "TTL_TEST", ttl: 100_000_000)
      assert config.max_age == 100_000_000
      assert config.duplicate_window == 100_000_000

      assert :ok = KV.delete_bucket(:gnat, "TTL_TEST")
    end

    test "creates a bucket with duplicate window > 2min" do
      assert {:ok, %{config: config}} =
               KV.create_bucket(:gnat, "OTHER_TTL_TEST", ttl: 1_300_000_000)

      assert config.max_age == 1_300_000_000
      assert config.duplicate_window == 1_200_000_000

      assert :ok = KV.delete_bucket(:gnat, "OTHER_TTL_TEST")
    end
  end

  test "create_key/4 creates a key" do
    assert {:ok, _} = KV.create_bucket(:gnat, "KEY_CREATE_TEST")
    assert :ok = KV.create_key(:gnat, "KEY_CREATE_TEST", "foo", "bar")
    assert "bar" = KV.get_value(:gnat, "KEY_CREATE_TEST", "foo")
    assert :ok = KV.delete_bucket(:gnat, "KEY_CREATE_TEST")
  end

  test "delete_key/3 deletes a key" do
    assert {:ok, _} = KV.create_bucket(:gnat, "KEY_DELETE_TEST")
    assert :ok = KV.create_key(:gnat, "KEY_DELETE_TEST", "foo", "bar")
    assert :ok = KV.delete_key(:gnat, "KEY_DELETE_TEST", "foo")
    refute KV.get_value(:gnat, "KEY_DELETE_TEST", "foo")
    assert :ok = KV.delete_bucket(:gnat, "KEY_DELETE_TEST")
  end

  test "purge_key/3 purges a key" do
    assert {:ok, _} = KV.create_bucket(:gnat, "KEY_PURGE_TEST")
    assert :ok = KV.create_key(:gnat, "KEY_PURGE_TEST", "foo", "bar")
    assert :ok = KV.purge_key(:gnat, "KEY_PURGE_TEST", "foo")
    refute KV.get_value(:gnat, "KEY_PURGE_TEST", "foo")
    assert :ok = KV.delete_bucket(:gnat, "KEY_PURGE_TEST")
  end

  test "put_value/4 updates a key" do
    assert {:ok, _} = KV.create_bucket(:gnat, "KEY_PUT_TEST")
    assert :ok = KV.create_key(:gnat, "KEY_PUT_TEST", "foo", "bar")
    assert :ok = KV.put_value(:gnat, "KEY_PUT_TEST", "foo", "baz")
    assert "baz" = KV.get_value(:gnat, "KEY_PUT_TEST", "foo")
    assert :ok = KV.delete_bucket(:gnat, "KEY_PUT_TEST")
  end

  describe "list_keys/2" do
    setup do
      bucket = "KEY_LIST_TEST"
      {:ok, _} = KV.create_bucket(:gnat, bucket)
      %{bucket: bucket}
    end

    test "provides all keys", %{bucket: bucket} do
      KV.put_value(:gnat, bucket, "foo", "bar")
      KV.put_value(:gnat, bucket, "baz", "quz")
      assert ["baz", "foo"] == KV.list_keys(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "deleted keys not included", %{bucket: bucket} do
      KV.put_value(:gnat, bucket, "foo", "bar")
      KV.put_value(:gnat, bucket, "baz", "quz")
      KV.delete_key(:gnat, bucket, "baz")
      assert ["foo"] == KV.list_keys(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end

    test "empty list for no keys", %{bucket: bucket} do
      assert [] == KV.list_keys(:gnat, bucket)
      :ok = KV.delete_bucket(:gnat, bucket)
    end
  end
end
