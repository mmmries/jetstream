defmodule Jetstream.API.StreamTest do
  use Jetstream.ConnCase
  alias Jetstream.API.KV

  @moduletag with_gnat: :gnat

  test "create_bucket/2 creates a bucket" do
    assert {:ok, %{config: config}} = KV.create_bucket(:gnat, "BUCKET_TEST")
    assert config.name == "KV_BUCKET_TEST"
    assert config.subjects == ["$KV.BUCKET_TEST.>"]
    assert config.max_msgs_per_subject == 1
    assert config.discard == :new
    assert config.allow_rollup_hdrs == true

    assert :ok = KV.delete_bucket(:gnat, "BUCKET_TEST")
  end

  test "create_key/4 creates a key" do
    assert {:ok, _} = KV.create_bucket(:gnat, "KEY_CREATE_TEST")
    assert :ok = KV.create_key(:gnat, "KEY_CREATE_TEST", "foo", "bar")
    assert "bar" = KV.get_value(:gnat, "KEY_CREATE_TEST", "foo")
  end
end
