defmodule Jetstream.PullConsumer.Server do
  @moduledoc false

  require Logger

  use Connection

  alias Jetstream.PullConsumer.{ConnectionOptions, Worker}

  defstruct [
    :connection_options,
    :connection_pid,
    :worker_init,
    :worker_pid,
    :module,
    current_retry: 0
  ]

  @impl Connection
  def init(%{module: module, init_arg: init_arg}) do
    _ = Process.put(:"$initial_call", {module, :init, 1})

    case module.init(init_arg) do
      {:ok, worker_init, connection_options} when is_list(connection_options) ->
        Process.flag(:trap_exit, true)

        connection_options = ConnectionOptions.validate!(connection_options)

        state = %__MODULE__{
          connection_options: connection_options,
          worker_init: worker_init,
          module: module
        }

        {:connect, :init, state}

      :ignore ->
        :ignore

      {:stop, _} = stop ->
        stop
    end
  end

  @impl Connection
  def connect(_info, %__MODULE__{} = state) do
    options = state.connection_options
    Logger.debug("#{ident(state)} is connecting to NATS")

    with {:ok, pid} <- connection_pid(options.connection_name),
         {:ok, _consumer} <- lookup_consumer(options),
         true <- Process.link(pid),
         {:ok, worker_pid} <- Worker.start_link(state.worker_init, state.module, options) do
      {:ok, %{state | connection_pid: pid, worker_pid: worker_pid}}
    else
      {:error, reason} ->
        if try_again?(state.current_retry, options.connection_retries) do
          Logger.error(
            "#{ident(state)} failed to connect to NATS and will retry. Reason: #{inspect(reason)}"
          )

          state = Map.update!(state, :current_retry, &(&1 + 1))
          {:backoff, options.connection_retry_timeout, state}
        else
          Logger.error(
            "#{ident(state)} failed to connect to NATS and retries limit has been exhausted. Stopping."
          )

          {:stop, :timeout, %{state | current_retry: 0}}
        end
    end
  end

  @impl Connection
  def disconnect({:close, from}, %__MODULE__{} = state) do
    Logger.debug("#{ident(state)} is disconnecting from NATS.")

    with true <- Process.unlink(state.connection_pid),
         true <- Process.unlink(state.worker_pid),
         true <- Process.exit(state.worker_pid, :brutal_kill) do
      Connection.reply(from, :ok)
      {:stop, :shutdown, %{state | connection_pid: nil, worker_pid: nil}}
    end
  end

  @impl Connection
  def handle_info({:EXIT, _pid, _reason}, %__MODULE__{} = state) do
    Logger.error("#{ident(state)}: NATS connection has died.")
    true = Process.unlink(state.worker_pid)
    true = Process.exit(state.worker_pid, :brutal_kill)
    {:connect, :reconnect, %{state | connection_pid: nil, worker_pid: nil}}
  end

  def handle_info(other, %__MODULE__{} = state) do
    Logger.debug("#{ident(state)} received unexpected message: #{inspect(other, pretty: true)}")

    {:noreply, state}
  end

  @impl Connection
  def handle_call(:close, _from, %__MODULE__{connection_pid: nil} = state) do
    {:stop, :shutdown, :ok, state}
  end

  def handle_call(:close, from, %__MODULE__{} = state) do
    Logger.debug("#{ident(state)} received :close call.")

    {:disconnect, {:close, from}, state}
  end

  defp ident(%__MODULE__{connection_options: options}) do
    ident(options)
  end

  defp ident(%ConnectionOptions{} = options) do
    "PullConsumer for #{options.stream_name}.#{options.consumer_name}"
  end

  def connection_pid(connection_name) when is_pid(connection_name) do
    if Process.alive?(connection_name) do
      {:ok, connection_name}
    else
      {:error, :connection_down}
    end
  end

  def connection_pid(connection_name) do
    case Process.whereis(connection_name) do
      nil -> {:error, :not_found}
      pid -> {:ok, pid}
    end
  end

  def lookup_consumer(%ConnectionOptions{} = options) do
    Jetstream.API.Consumer.info(
      options.connection_name,
      options.stream_name,
      options.consumer_name
    )
  end

  def try_again?(_attempt, :infinite), do: true
  def try_again?(attempt, max_retries) when attempt < max_retries, do: true
  def try_again?(_attempt, _max_retries), do: false
end
