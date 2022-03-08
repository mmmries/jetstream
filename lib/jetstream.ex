defmodule Jetstream do
  @moduledoc """
  Provides functions for interacting with a [NATS Jetstream](https://github.com/nats-io/jetstream)
  server.
  """

  @type message :: Gnat.message()

  @doc """
  Acknowledges a message was completely handled.
  """
  @spec ack(message :: message()) :: :ok
  def ack(message)

  def ack(%{gnat: gnat, reply_to: reply_to}) do
    Gnat.pub(gnat, reply_to, "")
  end

  @doc """
  Acknowledges the message was handled and requests delivery of the next message to the reply subject. Only applies
  to Pull-mode.
  """
  @spec ack_next(message :: message(), consumer_subject :: binary()) :: :ok
  def ack_next(message, consumer_subject)

  def ack_next(%{gnat: gnat, reply_to: reply_to}, consumer_subject) do
    Gnat.pub(gnat, reply_to, "+NXT", reply_to: consumer_subject)
  end

  @doc """
  Signals that the message will not be processed now and processing can move onto the next message, NAK'd message
  will be retried.
  """
  @spec nack(message :: message()) :: :ok
  def nack(message)

  def nack(%{gnat: gnat, reply_to: reply_to}) do
    Gnat.pub(gnat, reply_to, "-NAK")
  end
end
