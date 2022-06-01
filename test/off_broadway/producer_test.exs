defmodule OffBroadway.Jetstream.ProducerTest do
  use Jetstream.ConnCase

  alias Jetstream.API.{Consumer, Stream}

  defmodule Forwarder do
    use Broadway

    def start_link(opts) do
      Broadway.start_link(Forwarder, opts)
    end

    def handle_message(_, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data})
      message
    end

    def handle_batch(_, messages, _, _) do
      messages
    end
  end

  describe "Producer" do
    @describetag with_gnat: :gnat

    setup do
      stream_name = "BROADWAY_TEST_STREAM"
      stream_subjects = ["broadway"]
      consumer_name = "BROADWAY_TEST_CONSUMER"

      stream = %Stream{name: stream_name, subjects: stream_subjects}
      {:ok, _response} = Stream.create(:gnat, stream)

      consumer = %Consumer{stream_name: stream_name, durable_name: consumer_name}
      {:ok, _response} = Consumer.create(:gnat, consumer)

      producer_name = start_broadway(stream_name, consumer_name)

      %{producer_name: producer_name}
    end

    test "receive messages when the queue has less than the demand" do
      for i <- 1..5, do: {:ok, _} = Gnat.request(:gnat, "broadway", "message #{i}")

      for i <- 1..5 do
        expected_message = "message #{i}"

        assert_receive {:message_handled, ^expected_message}
      end
    end

    test "keep receiving messages when the queue has more than the demand" do
      for i <- 1..20, do: {:ok, _} = Gnat.request(:gnat, "broadway", "message #{i}")

      for i <- 1..20 do
        expected_message = "message #{i}"

        assert_receive {:message_handled, ^expected_message}
      end
    end

    test "keep trying to receive new messages when the queue is empty" do
      {:ok, _} = Gnat.request(:gnat, "broadway", "message 1")

      assert_receive {:message_handled, "message 1"}

      {:ok, _} = Gnat.request(:gnat, "broadway", "message 2")
      {:ok, _} = Gnat.request(:gnat, "broadway", "message 3")

      assert_receive {:message_handled, "message 2"}
      assert_receive {:message_handled, "message 3"}
    end

    test "stop trying to receive new messages after start draining", %{
      producer_name: producer_name
    } do
      [producer] = Broadway.producer_names(producer_name)

      :sys.suspend(producer)
      task = Task.async(fn -> Broadway.Topology.ProducerStage.drain(producer) end)
      :sys.resume(producer)
      Task.await(task)

      {:ok, _} = Gnat.request(:gnat, "broadway", "message")

      refute_receive {:message_handled, "message"}
    end
  end

  defp start_broadway(stream_name, consumer_name) do
    name = new_unique_name()

    start_supervised(
      {Forwarder,
       name: name,
       context: %{test_pid: self()},
       producer: [
         module: {
           OffBroadway.Jetstream.Producer,
           [
             receive_interval: 250,
             connection_name: :gnat,
             consumer_name: consumer_name,
             stream_name: stream_name,
             inbox_prefix: "_INBOX.",
             test_pid: self()
           ]
         },
         concurrency: 1
       ],
       processors: [
         default: [concurrency: 1]
       ],
       batchers: [
         default: [
           batch_size: 10,
           batch_timeout: 50,
           concurrency: 1
         ]
       ]}
    )

    name
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end
end
