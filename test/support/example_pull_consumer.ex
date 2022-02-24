defmodule Jetstream.ExamplePullConsumer do
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
