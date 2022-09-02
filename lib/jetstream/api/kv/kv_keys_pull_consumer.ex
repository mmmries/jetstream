defmodule Jetstream.API.KV.KVKeysPullConsumer do
  @moduledoc false
  # PullConsumer whose purpose is to get all the keys from a key value store

  use Jetstream.PullConsumer
  alias Jetstream.API.Consumer

  def start_link(conn, stream_name) do
    Jetstream.PullConsumer.start_link(__MODULE__, {conn, stream_name})
  end

  @impl true
  def init({conn, stream_name}) do
    consumer =
      Consumer.create(conn, %Consumer{
        # durable_name: "consumer",
        stream_name: stream_name
      })

    {:ok, %{keys: []},
     connection_name: conn, stream_name: stream_name, consumer_name: "TEST_CONSUMER"}
  end

  @impl true
  def handle_message(message, state) do
    # Do some processing with the message.
    {:ack, state}
  end
end
