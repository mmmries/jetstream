defmodule Jetstream.API.StreamTest do
  use Jetstream.ConnCase
  alias Jetstream.API.Stream

  @moduletag with_gnat: :gnat

  test "listing and creating, and deleting streams" do
    {:ok, %{streams: streams}} = Stream.list(:gnat)
    assert streams == nil || !("LIST_TEST" in streams)

    stream = %Stream{name: "LIST_TEST", subjects: ["STREAM_TEST"]}
    {:ok, response} = Stream.create(:gnat, stream)
    assert response.config == stream

    assert response.state == %{
             bytes: 0,
             consumer_count: 0,
             first_seq: 0,
             first_ts: ~U[0001-01-01 00:00:00Z],
             last_seq: 0,
             last_ts: ~U[0001-01-01 00:00:00Z],
             messages: 0,
             deleted: nil,
             lost: nil,
             num_deleted: nil,
             num_subjects: nil,
             subjects: nil
           }

    {:ok, %{streams: streams}} = Stream.list(:gnat)
    assert "LIST_TEST" in streams

    assert :ok = Stream.delete(:gnat, "LIST_TEST")
    {:ok, %{streams: streams}} = Stream.list(:gnat)
    assert streams == nil || !("LIST_TEST" in streams)
  end

  test "failed deletes" do
    assert {:error, %{"code" => 404, "description" => "stream not found"}} =
             Stream.delete(:gnat, "NaN")
  end

  test "getting stream info" do
    stream = %Stream{name: "INFO_TEST", subjects: ["INFO_TEST.*"]}
    assert {:ok, _response} = Stream.create(:gnat, stream)

    assert {:ok, response} = Stream.info(:gnat, "INFO_TEST")
    assert response.config == stream

    assert response.state == %{
             bytes: 0,
             consumer_count: 0,
             first_seq: 0,
             first_ts: ~U[0001-01-01 00:00:00Z],
             last_seq: 0,
             last_ts: ~U[0001-01-01 00:00:00Z],
             messages: 0,
             deleted: nil,
             lost: nil,
             num_deleted: nil,
             num_subjects: nil,
             subjects: nil
           }
  end

  test "creating a stream with non-standard settings" do
    stream = %Stream{
      name: "ARGS_TEST",
      subjects: ["ARGS_TEST.*"],
      retention: :workqueue,
      duplicate_window: 1_000_000,
      storage: :memory
    }

    assert {:ok, %{config: result}} = Stream.create(:gnat, stream)
    assert result.name == "ARGS_TEST"
    assert result.duplicate_window == 1_000_000
    assert result.retention == :workqueue
    assert result.storage == :memory
  end
end
