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
end
