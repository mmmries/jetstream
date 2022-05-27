with {:module, _} <- Code.ensure_compiled(Broadway) do
  defmodule Jetstream.Broadway.Producer do
    @moduledoc """
    A GenStage producer that continuously receives messages from a NATS JetStream
    and acknowledges them after being successfully processed.
    """

    use GenStage
    alias Broadway.Producer
    alias Jetstream.Broadway.Acknowledger

    @behaviour Producer

    @default_receive_interval 5000

    @impl true
    def init(opts) do
      receive_interval = opts[:receive_interval] || @default_receive_interval

      connection_name = opts[:connection_name]

      stream_name = opts[:stream_name]
      consumer_name = opts[:consumer_name]

      listening_topic = new_listening_topic(opts[:inbox_prefix])

      opts
      |> Acknowledger.init()
      |> case do
        {:ok, ack_ref} ->
          {:ok, sid} = Gnat.sub(connection_name, self(), listening_topic)

          {:producer,
           %{
             demand: 0,
             receive_timer: nil,
             receive_interval: receive_interval,
             connection_name: connection_name,
             stream_name: stream_name,
             consumer_name: consumer_name,
             listening_topic: listening_topic,
             subscription_id: sid,
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
    def prepare_for_start(_module, broadway_opts) do
      {producer_module, module_opts} = broadway_opts[:producer][:module]

      broadway_opts_with_defaults =
        put_in(broadway_opts, [:producer, :module], {producer_module, module_opts})

      {[], broadway_opts_with_defaults}
    end

    @impl true
    def handle_demand(incoming_demand, %{demand: demand} = state) do
      handle_receive_messages(%{state | demand: demand + incoming_demand})
    end

    @impl true
    def handle_info(:receive_messages, %{receive_timer: nil} = state) do
      {:noreply, [], state}
    end

    def handle_info(:receive_messages, state) do
      handle_receive_messages(%{state | receive_timer: nil})
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

    defp receive_messages_from_jetstream(state, _total_demand) do
      :ok =
        Gnat.pub(
          state.connection_name,
          "$JS.API.CONSUMER.MSG.NEXT.#{state.stream_name}.#{state.consumer_name}",
          %{batch: 1, no_wait: true} |> Jason.encode!(),
          reply_to: state.listening_topic
        )

      receive do
        {:msg, %{reply_to: "$JS.ACK" <> _} = msg} -> {:ok, msg}
      after
        5_000 ->
          {:error, :timeout}
      end
      |> case do
        {:error, _} = error ->
          IO.inspect(error)

          []

        {:ok, response} ->
          IO.inspect(response)

          []
      end
    end

    defp schedule_receive_messages(interval) do
      Process.send_after(self(), :receive_messages, interval)
    end
  end
end
