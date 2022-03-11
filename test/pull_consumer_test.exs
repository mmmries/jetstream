defmodule Jetstream.PullConsumerTest do
  use Jetstream.ConnCase

  alias Jetstream.API.{Consumer, Stream}

  defmodule ExamplePullConsumer do
    use Jetstream.PullConsumer

    def start_link(opts) do
      Jetstream.PullConsumer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      {:ok, nil, Keyword.merge([connection_name: :gnat], opts)}
    end

    @impl true
    def handle_message(%{topic: "ackable"}, state) do
      {:ack, state}
    end

    def handle_message(%{topic: "non-ackable", reply_to: reply_to}, state) do
      [_, _, _, _, delivered_count, _, _, _, _] = String.split(reply_to, ".")

      # NACK on first delivery
      if delivered_count == "1" do
        {:nack, state}
      else
        {:ack, state}
      end
    end

    def handle_message(%{topic: "skippable"}, state) do
      {:noreply, state}
    end
  end

  describe "PullConsumer" do
    @describetag with_gnat: :gnat

    setup do
      stream_name = "TEST_STREAM"
      stream_subjects = ["ackable", "non-ackable", "skippable"]
      consumer_name = "TEST_CONSUMER"

      stream = %Stream{name: stream_name, subjects: stream_subjects}
      {:ok, _response} = Stream.create(:gnat, stream)

      consumer = %Consumer{stream_name: stream_name, durable_name: consumer_name}
      {:ok, _response} = Consumer.create(:gnat, consumer)

      %{
        stream_name: stream_name,
        consumer_name: consumer_name
      }
    end

    test "ignores messages with :noreply", %{
      stream_name: stream_name,
      consumer_name: consumer_name
    } do
      start_supervised!(
        {ExamplePullConsumer, stream_name: stream_name, consumer_name: consumer_name}
      )

      :ok = Gnat.pub(:gnat, "skippable", "hello")

      refute_receive {:msg, _}
    end

    test "consumes JetStream messages", %{stream_name: stream_name, consumer_name: consumer_name} do
      start_supervised!(
        {ExamplePullConsumer, stream_name: stream_name, consumer_name: consumer_name}
      )

      Gnat.sub(:gnat, self(), "$JS.ACK.#{stream_name}.#{consumer_name}.>")

      :ok = Gnat.pub(:gnat, "ackable", "hello")

      assert_receive {:msg, %{body: "+NXT", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1")

      :ok = Gnat.pub(:gnat, "ackable", "hello")

      assert_receive {:msg, %{body: "+NXT", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1")

      :ok = Gnat.pub(:gnat, "non-ackable", "hello")

      assert_receive {:msg, %{body: "-NAK", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1")

      assert_receive {:msg, %{body: "+NXT", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.2")

      :ok = Gnat.pub(:gnat, "ackable", "hello")

      assert_receive {:msg, %{body: "+NXT", topic: topic}}
      assert String.starts_with?(topic, "$JS.ACK.#{stream_name}.#{consumer_name}.1")
    end

    test "can be manually closed", %{stream_name: stream_name, consumer_name: consumer_name} do
      pid =
        start_supervised!(
          {ExamplePullConsumer, stream_name: stream_name, consumer_name: consumer_name}
        )

      ref = Process.monitor(pid)

      assert :ok = Jetstream.PullConsumer.close(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
    end

    test "retries on unsuccessful connection", %{
      stream_name: stream_name,
      consumer_name: consumer_name
    } do
      pid =
        start_supervised!(
          {ExamplePullConsumer,
           connection_name: :non_existent,
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
