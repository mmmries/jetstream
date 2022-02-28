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
    setup do
      conn = start_supervised!({Gnat, %{}})

      stream_name = "TEST_STREAM"
      stream_subjects = ["ackable", "non-ackable", "skippable"]
      consumer_name = "TEST_CONSUMER"

      %{
        conn: conn,
        stream_name: stream_name,
        stream_subjects: stream_subjects,
        consumer_name: consumer_name
      }
    end

    for stream_variant <- [:existing, :non_existing],
        consumer_variant <- [:existing, :non_existing],
        !(stream_variant == :non_existing && consumer_variant == :existing) do
      @tag stream_variant: stream_variant
      @tag consumer_variant: consumer_variant
      test "consumes JetStream messages (stream #{stream_variant}, consumer #{consumer_variant})",
           %{
             stream_variant: stream_variant,
             consumer_variant: consumer_variant,
             conn: conn,
             stream_name: stream_name,
             stream_subjects: stream_subjects,
             consumer_name: consumer_name
           } do
        stream =
          if stream_variant == :existing do
            stream = %Stream{name: stream_name, subjects: stream_subjects}
            {:ok, _response} = Stream.create(conn, stream)
            stream_name
          else
            %Stream{name: stream_name, subjects: stream_subjects}
          end

        consumer =
          if consumer_variant == :existing do
            consumer = %Consumer{stream_name: stream_name, name: consumer_name}
            {:ok, _response} = Consumer.create(conn, consumer)
            consumer_name
          else
            %Consumer{stream_name: stream_name, name: consumer_name}
          end

        start_supervised!(
          {ExamplePullConsumer,
           %{
             connection_name: conn,
             stream: stream,
             consumer: consumer
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

        Consumer.delete(conn, stream_name, consumer_name)
      end
    end

    test "fails for non-existing stream and consumer", %{conn: conn} do
      ref =
        start_supervised!(
          {ExamplePullConsumer,
           %{
             connection_name: conn,
             stream: "wrong_stream",
             consumer: %Consumer{stream_name: "wrong_stream", name: "consumer"}
           }}
        )
        |> Process.monitor()

      assert_receive {:DOWN, ^ref, :process, _,
                      {:error, %{"code" => 404, "description" => "stream not found"}}}
    end
  end
end
