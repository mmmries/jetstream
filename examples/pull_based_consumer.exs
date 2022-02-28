# Start a nats server with jetstream enabled and default configs
# Now run the following snippets in an IEx terminal
alias Jetstream.API.{Consumer,Stream}

# Setup a connection to the nats server and create the stream/consumer
# This is the equivalen of these two nats cli commands
#   nats stream add TEST --subjects="greetings" --max-msgs=-1 --max-msg-size=-1 --max-bytes=-1 --max-age=-1 --storage=file --retention=limits --discard=old
#   nats consumer add TEST TEST --replay instant --deliver=all --ack all --wait=30s --filter="" --max-deliver=10
{:ok, connection} = Gnat.start_link()
stream = %Stream{name: "TEST", subjects: ["greetings"]}
{:ok, _response} = Stream.create(connection, stream)
consumer = %Consumer{stream_name: "TEST", name: "TEST", max_deliver: 10}
{:ok, _response} = Consumer.create(connection, consumer)

# Publish 100 messages into the stream
Enum.each(1..100, fn(i) -> Gnat.pub(connection, "greetings", "hello #{i}") end)

# normally you would add the `ConnectionSupervisor` and `PullConsumerPool` to your supervisrion tree
# here we start them up manually in an IEx session
{:ok, _pid} = Gnat.ConnectionSupervisor.start_link(%{
  name: :gnat,
  backoff_period: 4_000,
  connection_settings: [
    %{}
  ]
})

defmodule ExamplePullConsumer do
    use Jetstream.PullConsumer

    def handle_message(_message) do
      IO.inspect(message)
      :ack
    end
  end

Enum.each(1..10, fn(_) ->
  Jetstream.PullConsumer.start_link(
    ExamplePullConsumer,
    %{
      connection_name: :gnat,
      stream: "TEST",
      consumer: "TEST"
    }
  )
end)
