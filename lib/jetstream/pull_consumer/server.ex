defmodule Jetstream.PullConsumer.Server do
  @moduledoc false

  require Logger

  use Connection

  def init(%{module: module, init_arg: init_arg, options: options}) do
    _ = Process.put(:"$initial_call", {module, :init, 1})

    case module.init(init_arg) do
      {:ok, state} ->
        Process.flag(:trap_exit, true)

        initial_state = %{
          options: options,
          state: state,
          listening_topic: new_listening_topic(),
          module: module
        }

        {:connect, :init, initial_state}

      :ignore ->
        :ignore

      {:stop, _} = stop ->
        stop
    end
  end

  defp new_listening_topic do
    "_CON.#{nuid()}"
  end

  def connect(
        _,
        %{
          options: %{
            stream_name: stream_name,
            consumer_name: consumer_name,
            connection_name: connection_name,
            connection_retry_timeout: connection_retry_timeout,
            connection_retries: connection_retries
          },
          listening_topic: listening_topic,
          module: module
        } = state
      ) do
    Logger.debug(
      "#{__MODULE__} for #{stream_name}.#{consumer_name} is connecting to Gnat.",
      module: module,
      listening_topic: listening_topic,
      connection_name: connection_name
    )

    with {:ok, conn} <- connection_pid(connection_name),
         Process.link(conn),
         {:ok, sid} <- Gnat.sub(conn, self(), listening_topic),
         state = Map.put(state, :subscription_id, sid),
         :ok <- next_message(conn, stream_name, consumer_name, listening_topic),
         state = Map.delete(state, :current_retry) do
      {:ok, state}
    else
      {:error, reason} ->
        if Map.get(state, :current_retry, 0) >= connection_retries do
          Logger.error(
            """
            #{__MODULE__} for #{stream_name}.#{consumer_name} failed to connect to NATS and \
            retries limit has been exhausted. Stopping.
            """,
            module: module,
            listening_topic: listening_topic,
            connection_name: connection_name
          )

          {:stop, :timeout, Map.delete(state, :current_retry)}
        else
          Logger.debug(
            """
            #{__MODULE__} for #{stream_name}.#{consumer_name} failed to connect to Gnat \
            and will retry. Reason: #{inspect(reason)}
            """,
            module: module,
            listening_topic: listening_topic,
            connection_name: connection_name
          )

          state = Map.update(state, :current_retry, 0, fn retry -> retry + 1 end)
          {:backoff, connection_retry_timeout, state}
        end
    end
  end

  def disconnect(
        {:close, from},
        %{
          options: %{
            stream_name: stream_name,
            consumer_name: consumer_name,
            connection_name: connection_name
          },
          listening_topic: listening_topic,
          subscription_id: subscription_id,
          module: module
        } = state
      ) do
    Logger.debug(
      "#{__MODULE__} for #{stream_name}.#{consumer_name} is disconnecting from Gnat.",
      module: module,
      listening_topic: listening_topic,
      subscription_id: subscription_id,
      connection_name: connection_name
    )

    with {:ok, conn} <- connection_pid(connection_name),
         Process.unlink(conn),
         :ok <- Gnat.unsub(conn, subscription_id) do
      Logger.debug(
        "#{__MODULE__} for #{stream_name}.#{consumer_name} is shutting down.",
        module: module,
        listening_topic: listening_topic,
        subscription_id: subscription_id,
        connection_name: connection_name
      )

      Connection.reply(from, :ok)
      {:stop, :shutdown, state}
    end
  end

  defp connection_pid(connection_name) when is_pid(connection_name) do
    if Process.alive?(connection_name) do
      {:ok, connection_name}
    else
      {:error, :not_alive}
    end
  end

  defp connection_pid(connection_name) do
    case Process.whereis(connection_name) do
      nil -> {:error, :not_found}
      pid -> {:ok, pid}
    end
  end

  def handle_info({:msg, message}, %{options: options, module: module} = state) do
    Logger.debug(
      """
      #{__MODULE__} for #{options.stream_name}.#{options.consumer_name} received a message: \
      #{inspect(message, pretty: true)}
      """,
      module: module,
      listening_topic: state.listening_topic,
      subscription_id: state.subscription_id,
      connection_name: options.connection_name
    )

    case module.handle_message(message, state) do
      {:ack, state} ->
        Jetstream.ack_next(message, state.listening_topic)

        {:noreply, state}

      {:nack, state} ->
        Jetstream.nack(message)

        next_message(
          message.gnat,
          options.stream_name,
          options.consumer_name,
          state.listening_topic
        )

        {:noreply, state}

      {:noreply, state} ->
        next_message(
          message.gnat,
          options.stream_name,
          options.consumer_name,
          state.listening_topic
        )

        {:noreply, state}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, state) do
    Logger.debug(
      """
      #{__MODULE__} for #{state.options.stream_name}.#{state.options.consumer_name}: \
      NATS connection has died. PullConsumer is reconnecting.
      """,
      module: state.module,
      listening_topic: state.listening_topic,
      subscription_id: state.subscription_id,
      connection_name: state.options.connection_name
    )

    {:connect, :reconnect, state}
  end

  def handle_info(other, state) do
    Logger.debug(
      """
      #{__MODULE__} for #{state.options.stream_name}.#{state.options.consumer_name} received \
      unexpected message: #{inspect(other, pretty: true)}
      """,
      module: state.module,
      listening_topic: state.listening_topic,
      subscription_id: state.subscription_id,
      connection_name: state.options.connection_name
    )

    {:noreply, state}
  end

  def handle_call(:close, from, state) do
    Logger.debug(
      """
      #{__MODULE__} for #{state.options.stream_name}.#{state.options.consumer_name} received \
      :close call.
      """,
      module: state.module,
      listening_topic: state.listening_topic,
      subscription_id: state.subscription_id,
      connection_name: state.options.connection_name
    )

    {:disconnect, {:close, from}, state}
  end

  defp next_message(conn, stream_name, consumer_name, listening_topic) do
    Gnat.pub(
      conn,
      "$JS.API.CONSUMER.MSG.NEXT.#{stream_name}.#{consumer_name}",
      "1",
      reply_to: listening_topic
    )
  end

  defp nuid() do
    :crypto.strong_rand_bytes(12) |> Base.encode64()
  end
end
