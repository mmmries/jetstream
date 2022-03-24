defmodule Jetstream.API.ConsumerTest do
  use Jetstream.ConnCase
  alias Jetstream.API.{Consumer, Stream}

  @moduletag with_gnat: :gnat

  test "listing, creating, and deleting consumers" do
    stream = %Stream{name: "STREAM1", subjects: ["STREAM1"]}
    {:ok, _response} = Stream.create(:gnat, stream)

    assert {:ok, consumers} = Consumer.list(:gnat, "STREAM1")

    assert consumers == %{
             total: 0,
             offset: 0,
             limit: 1024,
             consumers: []
           }

    consumer = %Consumer{stream_name: "STREAM1", durable_name: "STREAM1"}
    assert {:ok, consumer_response} = Consumer.create(:gnat, consumer)

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
             replay_policy: :instant,
             backoff: nil,
             deliver_group: nil,
             description: nil,
             flow_control: nil,
             headers_only: nil,
             idle_heartbeat: nil,
             inactive_threshold: nil,
             max_ack_pending: 20000,
             max_batch: nil,
             max_deliver: -1,
             max_expires: nil,
             max_waiting: 512,
             rate_limit_bps: nil,
             sample_freq: nil
           }

    assert consumer_response.num_pending == 0
    assert consumer_response.num_redelivered == 0

    assert {:ok, consumers} = Consumer.list(:gnat, "STREAM1")

    assert consumers == %{
             total: 1,
             offset: 0,
             limit: 1024,
             consumers: ["STREAM1"]
           }

    assert :ok = Consumer.delete(:gnat, "STREAM1", "STREAM1")
    assert {:ok, consumers} = Consumer.list(:gnat, "STREAM1")

    assert consumers == %{
             total: 0,
             offset: 0,
             limit: 1024,
             consumers: []
           }
  end

  test "failed creates" do
    consumer = %Consumer{durable_name: "STREAM2", stream_name: "STREAM2"}

    assert {:error, %{"code" => 404, "description" => "stream not found"}} =
             Consumer.create(:gnat, consumer)
  end

  test "failed deletes" do
    assert {:error, %{"code" => 404, "description" => "stream not found"}} =
             Consumer.delete(:gnat, "STREAM3", "STREAM3")
  end

  test "getting consumer info" do
    stream = %Stream{name: "STREAM4", subjects: ["STREAM4"]}
    {:ok, _response} = Stream.create(:gnat, stream)

    consumer = %Consumer{
      stream_name: "STREAM4",
      durable_name: "STREAM4",
      deliver_subject: "consumer.STREAM4"
    }

    assert {:ok, _consumer_response} = Consumer.create(:gnat, consumer)

    assert {:ok, consumer_response} = Consumer.info(:gnat, "STREAM4", "STREAM4")

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
             deliver_subject: "consumer.STREAM4",
             durable_name: "STREAM4",
             filter_subject: nil,
             opt_start_seq: nil,
             opt_start_time: nil,
             replay_policy: :instant,
             backoff: nil,
             deliver_group: nil,
             description: nil,
             flow_control: nil,
             headers_only: nil,
             idle_heartbeat: nil,
             inactive_threshold: nil,
             max_ack_pending: 20000,
             max_batch: nil,
             max_deliver: -1,
             max_expires: nil,
             max_waiting: nil,
             rate_limit_bps: nil,
             sample_freq: nil
           }

    assert consumer_response.num_pending == 0
    assert consumer_response.num_redelivered == 0

    assert :ok = Consumer.delete(:gnat, "STREAM4", "STREAM4")
    assert :ok = Stream.delete(:gnat, "STREAM4")
  end
end
