defmodule Jetstream.PullConsumerTest do
  use ExUnit.Case

  alias Jetstream.API.{Consumer, Stream}

  defmodule ExamplePullConsumer do
    use Jetstream.PullConsumer

    def start_link(opts) do
      Jetstream.PullConsumer.start_link(__MODULE__, [], Keyword.new(opts))
    end

    def init(_) do
      {:ok, 0}
    end

    def handle_message(%{topic: "ackable"}, acc) do
      {:ack, acc + 1}
    end

    def handle_message(%{topic: "non-ackable", reply_to: reply_to}, acc) do
      [_, _, _, _, delivered_count, _, _, _, _] = String.split(reply_to, ".")

      # NACK on first delivery
      if delivered_count == "1" do
        {:nack, acc + 1}
      else
        {:ack, acc + 1}
      end
    end

    def handle_message(%{topic: "skippable"}, acc) do
      {:noreply, acc + 1}
    end
  end

  describe "PullConsumer" do
    setup do
      conn = start_supervised!({Gnat, %{}})

      stream_name = "TEST_STREAM"
      stream_subjects = ["ackable", "non-ackable", "skippable"]
      consumer_name = "TEST_CONSUMER"

      stream = %Stream{name: stream_name, subjects: stream_subjects}
      {:ok, _response} = Stream.create(conn, stream)

      consumer = %Consumer{stream_name: stream_name, durable_name: consumer_name}
      {:ok, _response} = Consumer.create(conn, consumer)

      %{
        conn: conn,
        stream_name: stream_name,
        consumer_name: consumer_name
      }
    end

    test "ignores messages with :noreply", %{
      conn: conn,
      stream_name: stream_name,
      consumer_name: consumer_name
    } do
      start_supervised!(
        {ExamplePullConsumer,
         connection_name: conn, stream_name: stream_name, consumer_name: consumer_name}
      )

      :ok = Gnat.pub(conn, "skippable", "hello")

      refute_receive {:msg, _}
    end

    test "consumes JetStream messages",
         %{
           conn: conn,
           stream_name: stream_name,
           consumer_name: consumer_name
         } do
      start_supervised!(
        {ExamplePullConsumer,
         connection_name: conn, stream_name: stream_name, consumer_name: consumer_name}
      )

      Gnat.sub(conn, self(), "$JS.ACK.#{stream_name}.#{consumer_name}.>")

      :ok = Gnat.pub(conn, "ackable", "hello")

      assert_receive {:msg, %{body: "+NXT", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1")

      :ok = Gnat.pub(conn, "ackable", "hello")

      assert_receive {:msg, %{body: "+NXT", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1")

      :ok = Gnat.pub(conn, "non-ackable", "hello")

      assert_receive {:msg, %{body: "-NAK", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1")

      assert_receive {:msg, %{body: "+NXT", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.2")

      :ok = Gnat.pub(conn, "ackable", "hello")

      assert_receive {:msg, %{body: "+NXT", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1")
    end

    test "can be manually closed", %{
      conn: conn,
      stream_name: stream_name,
      consumer_name: consumer_name
    } do
      pid =
        start_supervised!(
          {ExamplePullConsumer,
           connection_name: conn, stream_name: stream_name, consumer_name: consumer_name}
        )

      ref = Process.monitor(pid)

      assert :ok = Jetstream.PullConsumer.close(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
    end

    test "retries on unsucessful connection", %{
      stream_name: stream_name,
      consumer_name: consumer_name
    } do
      pid =
        start_supervised!(
          {ExamplePullConsumer,
           connection_name: :gnat,
           stream_name: stream_name,
           consumer_name: consumer_name,
           connection_retry_timeout: 50,
           connection_retries: 2},
          restart: :temporary
        )

      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :timeout}, 1_000
    end
  end
end
