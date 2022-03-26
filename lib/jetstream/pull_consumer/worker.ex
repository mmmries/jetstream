defmodule Jetstream.PullConsumer.Worker do
  @moduledoc false

  use GenServer

  alias Jetstream.PullConsumer.ConnectionOptions

  require Logger

  def start_link(init_arg, module, connection_options) do
    GenServer.start_link(__MODULE__, {init_arg, module, connection_options})
  end

  def init({init_arg, module, connection_options = %ConnectionOptions{}}) do
    o = connection_options
    listening_topic = o.inbox_prefix <> nuid()
    {:ok, sid} = Gnat.sub(o.connection_name, self(), listening_topic)

    state = %{
      conn: o.connection_name,
      next_message_topic: "$JS.API.CONSUMER.MSG.NEXT.#{o.stream_name}.#{o.consumer_name}",
      stream_name: o.stream_name,
      consumer_name: o.consumer_name,
      listening_topic: listening_topic,
      module: module,
      sid: sid,
      callback_state: init_arg
    }

    next_message(state)

    {:ok, state}
  end

  def handle_info({:msg, message}, state) do
    case state.module.handle_message(message, state.callback_state) do
      {:ack, callback_state} ->
        Jetstream.ack_next(message, state.listening_topic)
        {:noreply, %{state | callback_state: callback_state}}

      {:nack, callback_state} ->
        Jetstream.nack(message)
        next_message(state)
        {:noreply, %{state | callback_state: callback_state}}

      {:noreply, callback_state} ->
        next_message(state)
        {:noreply, %{state | callback_state: callback_state}}
    end
  end

  defp next_message(state) do
    Logger.debug("Worker next_message #{inspect(state)}")

    Gnat.pub(
      state.conn,
      state.next_message_topic,
      "1",
      reply_to: state.listening_topic
    )
  end

  defp nuid() do
    :crypto.strong_rand_bytes(12) |> Base.encode64()
  end
end
