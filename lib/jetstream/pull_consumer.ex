defmodule Jetstream.PullConsumer do
  use GenServer

  def init(%{
        connection_pid: connection_pid,
        stream_name: stream_name,
        consumer_name: consumer_name
      }) do
    Process.link(connection_pid)
    listening_topic = "_CON.#{nuid()}"
    {:ok, _sid} = Gnat.sub(connection_pid, self(), listening_topic)

    :ok =
      Gnat.pub(connection_pid, "$JS.API.CONSUMER.MSG.NEXT.#{stream_name}.#{consumer_name}", "1",
        reply_to: listening_topic
      )

    state = %{
      stream_name: stream_name,
      consumer_name: consumer_name,
      listening_topic: listening_topic
    }

    {:ok, state}
  end

  def handle_info({:msg, message}, state) do
    IO.puts(message.body)
    :timer.sleep(1_000)
    Jetstream.ack_next(message, state.listening_topic)
    Gnat.pub(message.gnat, message.reply_to, "+NXT", reply_to: state.listening_topic)
    {:noreply, state}
  end

  def handle_info(other, state) do
    require Logger

    Logger.error("""
    #{__MODULE__} for #{state.stream_name}.#{state.consumer_name} received unexpected message:
    #{inspect(other)}
    """)

    {:noreply, state}
  end

  defp nuid(), do: :crypto.strong_rand_bytes(12) |> Base.encode64()
end
