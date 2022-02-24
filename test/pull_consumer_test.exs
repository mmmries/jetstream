defmodule Jetstream.PullConsumerTest do
  use ExUnit.Case

  alias Jetstream.API.{Consumer, Stream}

  defmodule ExamplePullConsumer do
    use Jetstream.PullConsumer

    def handle_message(%{topic: "ackable"}) do
      :ack
    end

    def handle_message(%{topic: "non-ackable"}) do
      :nack
    end

    def handle_message(%{topic: "skippable"}) do
      :noreply
    end
  end

  describe "PullConsumer" do
    test "consumes JetStream messages" do
      conn = start_supervised!({Gnat, %{}})

      stream_name = "TEST_STREAM"
      stream_subjects = ["ackable", "non-ackable", "skippable"]
      consumer_name = "TEST_CONSUMER"

      stream = %Stream{name: stream_name, subjects: stream_subjects}
      {:ok, _response} = Stream.create(conn, stream)
      consumer = %Consumer{stream_name: stream_name, name: consumer_name, max_deliver: 10}
      {:ok, _response} = Consumer.create(conn, consumer)

      start_supervised!(
        {ExamplePullConsumer,
         %{
           connection_name: conn,
           stream_name: stream_name,
           consumer_name: consumer_name
         }}
      )

      Gnat.sub(conn, self(), "$JS.ACK.#{stream_name}.#{consumer_name}.>")

      :ok = Gnat.pub(conn, "ackable", "hello")

      assert_receive {:msg, %{body: "+NXT", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1.1.1.")

      :ok = Gnat.pub(conn, "ackable", "hello")

      assert_receive {:msg, %{body: "+NXT", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1.2.2.")

      :ok = Gnat.pub(conn, "non-ackable", "hello")

      assert_receive {:msg, %{body: "-NAK", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1.3.3.")

      :ok = Gnat.pub(conn, "skippable", "hello")

      refute_receive {:msg, _}

      :ok = Gnat.pub(conn, "other", "hello")

      refute_receive {:msg, _}
    end
  end
end
