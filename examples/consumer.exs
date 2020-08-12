# One-time Setup
{:ok, gnat} = Gnat.start_link()
stream = %Jetstream.Stream{name: "TEST", subjects: ["greetings.*"]}
{:ok, _} = Jetstream.Stream.create(gnat, stream)
# setup a consumer consumer using the `nats` cli tool
# nats consumer add greetings a --target consumer.greetings --replay instant --deliver=all --ack all --wait=5s --filter="" --max-deliver=10

# Setup Consumer/Subscription
defmodule Subscriber do
  def handle(%{reply_to: reply_to, gnat: gnat} = msg) do
    IO.inspect(msg)
    case msg.body do
      "hola" -> Gnat.pub(gnat, reply_to, "")
      "bom dia" -> Gnat.pub(gnat, reply_to, "-NAK")
      _ -> nil
    end
  end
end

{:ok, _pid} = Gnat.ConnectionSupervisor.start_link(%{
  name: :gnat,
  backoff_period: 4_000,
  connection_settings: [
    %{}
  ]
})
{:ok, _pid} = Gnat.ConsumerSupervisor.start_link(%{
  connection_name: :gnat,
  consuming_function: {Subscriber, :handle},
  subscription_topics: [
    %{topic: "consumer.TEST"}
  ]
})

# now publish some messages into the stream

Gnat.pub(gnat, "greetings.en", "hello")
Gnat.pub(gnat, "greetings.sp", "hola")
Gnat.pub(gnat, "greetings.pr", "bom dia")
