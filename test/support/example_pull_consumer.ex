defmodule Jetstream.ExamplePullConsumer do
  use Jetstream.PullConsumer

  def handle_message("ackable", _message) do
    :ack
  end

  def handle_message("non-ackable", _message) do
    :nack
  end

  def handle_message("skippable", _message) do
    :noreply
  end
end
