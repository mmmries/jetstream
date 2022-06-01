with {:module, _} <- Code.ensure_compiled(Broadway) do
  defmodule OffBroadway.Jetstream.Producer do
    @moduledoc """
    A GenStage producer that continuously receives messages from a NATS JetStream
    and acknowledges them after being successfully processed.

    ## Options

    ### Connection options

    * `connection_name` - The name of Gnat process or Gnat connection supervisor.

    * `stream_name` - The name of stream to consume from.

    * `consumer_name` - The name of consumer.

    ### Message pulling options

    * `receive_interval` - The duration in milliseconds for which the producer waits
      before making a request for more messages. Defaults to 5000.

    * `receive_timeout` - The maximum time to wait for NATS Jetstream to respond with
       a requested message. Defaults to `:infinity`.

    ### Acknowledger options

    * `:on_success` - Configures the behaviour for successful messages. Defaults to `:ack`.

    * `:on_failure` - Configures the behaviour for failed messages. Defaults to `:nack`.
    """

    use GenStage

    alias Broadway.Message
    alias Broadway.Producer
    alias OffBroadway.Jetstream.Acknowledger

    @behaviour Producer

    @default_receive_interval 5_000
    @default_receive_timeout :infinity

    @impl Producer
    def prepare_for_start(_module, broadway_opts) do
      {producer_module, module_opts} = broadway_opts[:producer][:module]

      broadway_opts_with_defaults =
        put_in(broadway_opts, [:producer, :module], {producer_module, module_opts})

      {[], broadway_opts_with_defaults}
    end

    @impl true
    def init(opts) do
      receive_interval = opts[:receive_interval] || @default_receive_interval
      receive_timeout = opts[:receive_timeout] || @default_receive_timeout

      connection_name = opts[:connection_name]

      stream_name = opts[:stream_name]
      consumer_name = opts[:consumer_name]

      listening_topic = new_listening_topic(opts[:inbox_prefix])

      case Acknowledger.init(opts) do
        {:ok, ack_ref} ->
          send(self(), :connect)

          {:producer,
           %{
             demand: 0,
             receive_timer: nil,
             receive_interval: receive_interval,
             receive_timeout: receive_timeout,
             connection_name: connection_name,
             connection_pid: nil,
             status: :disconnected,
             stream_name: stream_name,
             consumer_name: consumer_name,
             listening_topic: listening_topic,
             ack_ref: ack_ref
           }}

        {:error, message} ->
          raise ArgumentError, message
      end
    end

    defp new_listening_topic(inbox_prefix) do
      inbox_prefix <> nuid()
    end

    defp nuid do
      :crypto.strong_rand_bytes(12) |> Base.encode64()
    end

    @impl true
    def handle_demand(incoming_demand, %{demand: demand} = state) do
      handle_receive_messages(%{state | demand: demand + incoming_demand})
    end

    @impl true
    def handle_info(
          :connect,
          %{connection_name: connection_name, listening_topic: listening_topic} = state
        ) do
      case Process.whereis(connection_name) do
        nil ->
          Process.send_after(self(), :connect, 2_000)
          {:noreply, [], state}

        connection_pid ->
          Process.monitor(connection_pid)

          {:ok, _sid} = Gnat.sub(connection_name, self(), listening_topic)

          {:noreply, [], %{state | status: :connected, connection_pid: connection_pid}}
      end
    end

    def handle_info(:receive_messages, %{status: :disconnected} = state) do
      {:noreply, [], state}
    end

    def handle_info(:receive_messages, %{receive_timer: nil} = state) do
      {:noreply, [], state}
    end

    def handle_info(:receive_messages, state) do
      handle_receive_messages(%{state | receive_timer: nil})
    end

    def handle_info(
          {:DOWN, _ref, :process, connection_pid, _reason},
          %{connection_pid: connection_pid} = state
        ) do
      Process.send_after(self(), :connect, 2_000)
      {:noreply, [], %{state | status: :disconnected, connection_pid: nil}}
    end

    def handle_info(_, state) do
      {:noreply, [], state}
    end

    @impl true
    def prepare_for_draining(%{receive_timer: receive_timer} = state) do
      receive_timer && Process.cancel_timer(receive_timer)
      {:noreply, [], %{state | receive_timer: nil}}
    end

    defp handle_receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
      messages = receive_messages_from_jetstream(state, demand)
      new_demand = demand - length(messages)

      receive_timer =
        case {messages, new_demand} do
          {[], _} -> schedule_receive_messages(state.receive_interval)
          {_, 0} -> nil
          _ -> schedule_receive_messages(0)
        end

      {:noreply, messages, %{state | demand: new_demand, receive_timer: receive_timer}}
    end

    defp handle_receive_messages(state) do
      {:noreply, [], state}
    end

    defp receive_messages_from_jetstream(state, total_demand) do
      request_messages_from_jetstream(total_demand, state)

      do_receive_messages(total_demand, state.listening_topic, state.receive_timeout)
      |> wrap_received_messages(state.ack_ref)
    end

    defp request_messages_from_jetstream(total_demand, state) do
      Jetstream.API.Consumer.next_message(
        state.connection_name,
        state.stream_name,
        state.consumer_name,
        state.listening_topic,
        total_demand,
        true
      )
    end

    defp do_receive_messages(total_demand, listening_topic, receive_timeout) do
      Enum.reduce_while(1..total_demand, [], fn _, acc ->
        receive do
          {:msg, %{reply_to: "$JS.ACK" <> _} = msg} ->
            {:cont, [msg | acc]}

          {:msg, %{topic: ^listening_topic}} ->
            {:halt, acc}
        after
          receive_timeout ->
            {:halt, acc}
        end
      end)
    end

    defp wrap_received_messages(jetstream_messages, ack_ref) do
      Enum.map(jetstream_messages, &jetstream_msg_to_broadway_msg(&1, ack_ref))
    end

    defp jetstream_msg_to_broadway_msg(jetstream_message, ack_ref) do
      acknowledger = Acknowledger.builder(ack_ref).(jetstream_message.reply_to)

      %Message{
        data: jetstream_message.body,
        metadata: %{
          topic: jetstream_message.topic
        },
        acknowledger: acknowledger
      }
    end

    defp schedule_receive_messages(interval) do
      Process.send_after(self(), :receive_messages, interval)
    end
  end
end
