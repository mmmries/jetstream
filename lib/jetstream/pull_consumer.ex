defmodule Jetstream.PullConsumer do
  use GenServer

  @callback handle_message(topic :: String.t(), message :: Gnat.message()) ::
              :ack | :nack | :noreply

  defmacro __using__(_opts) do
    quote do
      @behaviour Jetstream.PullConsumer

      def init(init_arg) do
        init_arg
        |> Map.put(:module, __MODULE__)
        |> Jetstream.PullConsumer.init()
      end

      def child_spec(init_arg) do
        init_arg
        |> Map.put(:module, __MODULE__)
        |> Jetstream.PullConsumer.child_spec()
      end

      def handle_info(msg, state) do
        Jetstream.PullConsumer.handle_info(msg, state)
      end
    end
  end

  def start_link(settings, options \\ []) do
    GenServer.start_link(__MODULE__, settings, options)
  end

  def init(%{
        connection_name: connection_name,
        stream_name: stream_name,
        consumer_name: consumer_name,
        module: module
      }) do
    Process.link(connection_name)
    listening_topic = "_CON.#{nuid()}"
    {:ok, _sid} = Gnat.sub(connection_name, self(), listening_topic)

    :ok =
      Gnat.pub(connection_name, "$JS.API.CONSUMER.MSG.NEXT.#{stream_name}.#{consumer_name}", "1",
        reply_to: listening_topic
      )

    state = %{
      stream_name: stream_name,
      consumer_name: consumer_name,
      listening_topic: listening_topic,
      module: module
    }

    {:ok, state}
  end

  def handle_info({:msg, message}, state) do
    case state.module.handle_message(message.topic, message.body) do
      :ack -> Jetstream.ack_next(message, state.listening_topic)
      :nack -> Jetstream.nack(message)
      :noreply -> nil
    end

    {:noreply, state}
  end

  def handle_info(other, state) do
    require Logger

    Logger.error(
      "#{__MODULE__} for #{state.stream_name}.#{state.consumer_name} received unexpected message: #{inspect(other)}"
    )

    {:noreply, state}
  end

  defp nuid(), do: :crypto.strong_rand_bytes(12) |> Base.encode64()
end
