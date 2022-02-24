defmodule Jetstream do
  @moduledoc """
  Provides functions for interacting with a [NATS Jetstream](https://github.com/nats-io/jetstream) server.

  > Note: Jetstream is currently considered a beta feature of the NATS project, it is not available by default.
  > Any APIs you see here are liable to change in a way that is outside the normal versioning scheme.
  """

  @type message :: Gnat.message()

  @spec ack(message()) :: :ok
  def ack(%{gnat: gnat, reply_to: reply_to}) do
    Gnat.pub(gnat, reply_to, "")
  end

  @spec ack_next(message(), binary()) :: :ok
  def ack_next(%{gnat: gnat, reply_to: reply_to}, consumer_topic) do
    Gnat.pub(gnat, reply_to, "+NXT", reply_to: consumer_topic)
  end

  @spec nack(message()) :: :ok
  def nack(%{gnat: gnat, reply_to: reply_to}) do
    Gnat.pub(gnat, reply_to, "-NAK")
  end
end
