defmodule Jetstream.Broadway.ProducerTest do
  use Jetstream.ConnCase

  alias Jetstream.API.{Consumer, Stream}

  defmodule Forwarder do
    use Broadway

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
      stream_subjects = ["ack", "nack", "skip"]
      consumer_name = "BROADWAY_TEST_CONSUMER"

      stream = %Stream{name: stream_name, subjects: stream_subjects}
      {:ok, _response} = Stream.create(:gnat, stream)

      consumer = %Consumer{stream_name: stream_name, durable_name: consumer_name}
      {:ok, _response} = Consumer.create(:gnat, consumer)

      %{
        stream_name: stream_name,
        consumer_name: consumer_name
      }
    end

    test "receives messages", %{stream_name: stream_name, consumer_name: consumer_name} do
      start_broadway(stream_name, consumer_name)

      Process.sleep(1_000)

      for _ <- 1..10, do: {:ok, _} = Gnat.request(:gnat, "ack", "hello") |> IO.inspect()

      Process.sleep(11_000)

      for _ <- 1..10, do: {:ok, _} = Gnat.request(:gnat, "ack", "hello") |> IO.inspect()

      Process.sleep(11_000)
    end
  end

  defp start_broadway(stream_name, consumer_name) do
    name = new_unique_name()

    {:ok, _pid} =
      Broadway.start_link(
        Forwarder,
        name: name,
        context: %{test_pid: self()},
        producer: [
          module: {
            Jetstream.Broadway.Producer,
            [
              receive_interval: 1_000,
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
        ]
      )

    name
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end
end
