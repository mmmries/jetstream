defmodule Jetstream.PullConsumer.Server do
  @moduledoc false

  require Logger

  use Connection

  def init(%{module: module, init_arg: init_arg, options: options}) do
    _ = Process.put(:"$initial_call", {module, :init, 1})

    case module.init(init_arg) do
      {:ok, state} ->
        Process.flag(:trap_exit, true)

        gen_state = %{
          options: options,
          state: state,
          listening_topic: new_listening_topic(),
          module: module
        }

        {:connect, :init, gen_state}

      :ignore ->
        :ignore

      {:stop, _} = stop ->
        stop
    end
  end

  defp new_listening_topic do
    "_CON.#{nuid()}"
  end

  defp nuid() do
    :crypto.strong_rand_bytes(12) |> Base.encode64()
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
        } = gen_state
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
         gen_state = Map.put(gen_state, :subscription_id, sid),
         :ok <- next_message(conn, stream_name, consumer_name, listening_topic),
         gen_state = Map.delete(gen_state, :current_retry) do
      {:ok, gen_state}
    else
      {:error, reason} ->
        if Map.get(gen_state, :current_retry, 0) >= connection_retries do
          Logger.error(
            """
            #{__MODULE__} for #{stream_name}.#{consumer_name} failed to connect to NATS and \
            retries limit has been exhausted. Stopping.
            """,
            module: module,
            listening_topic: listening_topic,
            connection_name: connection_name
          )

          {:stop, :timeout, Map.delete(gen_state, :current_retry)}
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

          gen_state = Map.update(gen_state, :current_retry, 0, fn retry -> retry + 1 end)
          {:backoff, connection_retry_timeout, gen_state}
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
        } = gen_state
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
      {:stop, :shutdown, gen_state}
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

  def handle_info({:msg, message}, %{options: options, state: state, module: module} = gen_state) do
    Logger.debug(
      """
      #{__MODULE__} for #{options.stream_name}.#{options.consumer_name} received a message: \
      #{inspect(message, pretty: true)}
      """,
      module: module,
      listening_topic: gen_state.listening_topic,
      subscription_id: gen_state.subscription_id,
      connection_name: options.connection_name
    )

    case module.handle_message(message, state) do
      {:ack, state} ->
        Jetstream.ack_next(message, gen_state.listening_topic)

        gen_state = %{gen_state | state: state}
        {:noreply, gen_state}

      {:nack, state} ->
        Jetstream.nack(message)

        next_message(
          message.gnat,
          options.stream_name,
          options.consumer_name,
          gen_state.listening_topic
        )

        gen_state = %{gen_state | state: state}
        {:noreply, gen_state}

      {:noreply, state} ->
        next_message(
          message.gnat,
          options.stream_name,
          options.consumer_name,
          gen_state.listening_topic
        )

        gen_state = %{gen_state | state: state}
        {:noreply, gen_state}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, gen_state) do
    Logger.debug(
      """
      #{__MODULE__} for #{gen_state.options.stream_name}.#{gen_state.options.consumer_name}: \
      NATS connection has died. PullConsumer is reconnecting.
      """,
      module: gen_state.module,
      listening_topic: gen_state.listening_topic,
      subscription_id: gen_state.subscription_id,
      connection_name: gen_state.options.connection_name
    )

    {:connect, :reconnect, gen_state}
  end

  def handle_info(other, gen_state) do
    Logger.debug(
      """
      #{__MODULE__} for #{gen_state.options.stream_name}.#{gen_state.options.consumer_name} received \
      unexpected message: #{inspect(other, pretty: true)}
      """,
      module: gen_state.module,
      listening_topic: gen_state.listening_topic,
      subscription_id: gen_state.subscription_id,
      connection_name: gen_state.options.connection_name
    )

    {:noreply, gen_state}
  end

  def handle_call(:close, from, gen_state) do
    Logger.debug(
      """
      #{__MODULE__} for #{gen_state.options.stream_name}.#{gen_state.options.consumer_name} received \
      :close call.
      """,
      module: gen_state.module,
      listening_topic: gen_state.listening_topic,
      subscription_id: gen_state.subscription_id,
      connection_name: gen_state.options.connection_name
    )

    {:disconnect, {:close, from}, gen_state}
  end

  defp next_message(conn, stream_name, consumer_name, listening_topic) do
    Gnat.pub(
      conn,
      "$JS.API.CONSUMER.MSG.NEXT.#{stream_name}.#{consumer_name}",
      "1",
      reply_to: listening_topic
    )
  end
end