defmodule Jetstream.ConsumerTest do
  use ExUnit.Case
  alias Jetstream.{Consumer,Stream}

  test "listing, creating, and deleting consumers" do
    conn = gnat()
    stream = %Stream{name: "STREAM1", subjects: ["STREAM1"]}
    {:ok, _response} = Stream.create(conn, stream)

    assert {:ok, consumers} = Consumer.list(conn, "STREAM1")
    assert consumers == %{
      total: 0,
      offset: 0,
      limit: 1024,
      consumers: []
    }

    consumer = %Consumer{stream_name: "STREAM1", name: "STREAM1"}
    assert {:ok, consumer_response} = Consumer.create(conn, consumer)
    assert consumer_response.ack_floor == %{
      consumer_seq: 0,
      stream_seq: 0
    }
    assert consumer_response.delivered == %{
      consumer_seq: 0,
      stream_seq: 0
    }
    assert %DateTime{} = consumer_response.created
    assert consumer_response.config == %{
      ack_policy: :explicit,
      ack_wait: 30_000_000_000,
      deliver_policy: :all,
      deliver_subject: nil,
      durable_name: "STREAM1",
      filter_subject: nil,
      opt_start_seq: nil,
      opt_start_time: nil,
      replay_policy: :instant
    }
    assert consumer_response.num_pending == 0
    assert consumer_response.num_redelivered == 0

    assert {:ok, consumers} = Consumer.list(conn, "STREAM1")
    assert consumers == %{
      total: 1,
      offset: 0,
      limit: 1024,
      consumers: ["STREAM1"]
    }

    assert :ok = Consumer.delete(conn, "STREAM1", "STREAM1")
    assert {:ok, consumers} = Consumer.list(conn, "STREAM1")
    assert consumers == %{
      total: 0,
      offset: 0,
      limit: 1024,
      consumers: []
    }
  end

  defp gnat do
    {:ok, pid} = Gnat.start_link()
    pid
  end
end
