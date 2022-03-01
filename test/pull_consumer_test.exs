defmodule Jetstream.PullConsumerTest do
  use ExUnit.Case

  alias Jetstream.API.{Consumer, Stream}

  defmodule ExamplePullConsumer do
    use Jetstream.PullConsumer

    def handle_message(%{topic: "ackable"}) do
      :ack
    end

    def handle_message(%{topic: "non-ackable", reply_to: reply_to}) do
      [_, _, _, _, delivered_count, _, _, _, _] = String.split(reply_to, ".")

      # NACK on first delivery
      if delivered_count == "1", do: :nack, else: :ack
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

    test "ignores messages with :noreply", %{
      conn: conn,
      stream_name: stream_name,
      stream_subjects: stream_subjects,
      consumer_name: consumer_name
    } do
      start_supervised!(
        {ExamplePullConsumer,
         %{
           connection_name: conn,
           stream: %{name: stream_name, subjects: stream_subjects},
           consumer: %{stream_name: stream_name, durable_name: consumer_name}
         }}
      )

      :ok = Gnat.pub(conn, "skippable", "hello")

      refute_receive {:msg, _}
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
            %{name: stream_name, subjects: stream_subjects}
          end

        consumer =
          if consumer_variant == :existing do
            consumer = %Consumer{stream_name: stream_name, durable_name: consumer_name}
            {:ok, _response} = Consumer.create(conn, consumer)
            consumer_name
          else
            %{stream_name: stream_name, durable_name: consumer_name}
          end

        if stream_variant == :non_existing do
          {:ok, _} = Gnat.sub(conn, self(), "$JS.API.STREAM.CREATE.#{stream_name}")
        end

        if consumer_variant == :non_existing do
          {:ok, _} =
            Gnat.sub(
              conn,
              self(),
              "$JS.API.CONSUMER.DURABLE.CREATE.#{stream_name}.#{consumer_name}"
            )
        end

        start_supervised!(
          {ExamplePullConsumer,
           %{
             connection_name: conn,
             stream: stream,
             consumer: consumer
           }}
        )

        if stream_variant == :non_existing do
          assert_receive {:msg, %{topic: "$JS.API.STREAM.CREATE." <> _}}
        end

        if consumer_variant == :non_existing do
          assert_receive {:msg, %{topic: "$JS.API.CONSUMER.DURABLE.CREATE." <> _}}
        end

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
    end

    test "fails for non-existing stream and consumer", %{conn: conn} do
      ref =
        start_supervised!(
          {ExamplePullConsumer,
           %{
             connection_name: conn,
             stream: "wrong_stream",
             consumer: %{stream_name: "wrong_stream", durable_name: "consumer"}
           }}
        )
        |> Process.monitor()

      assert_receive {:DOWN, ^ref, :process, _,
                      {:error, %{"code" => 404, "description" => "stream not found"}}}
    end
  end
end
