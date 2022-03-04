defmodule Jetstream.PullConsumer do
  @moduledoc """
  A behaviour which provides the NATS JetStream Pull Consumer functionalities.

  When a Consumer is pull-based, it means that the messages will be delivered when the server is asked
  for them.

  ## Usage

  Declare a module which uses `Jetstream.PullConsumer` and implements the `handle_message/1` function.

  ```
  defmodule MyApp.PullConsumer do
    use Jetstream.PullConsumer

    def handle_message(message) do
      # Do some processing with the message.
      :ack
    end
  end
  ```

  You can then place your Pull Consumer in a supervision tree. Remember that you need to have the
  `Gnat.ConnectionSupervisor` set up.

  ```
  defmodule MyApp.Application do
    use Application

    @impl true
    def start(_type, _args) do
      children = [
        # Create NATS connection
        {Gnat.ConnectionSupervisor, ...},
        # Start NATS Jetstream Pull Consumer
        {MyApp.PullConsumer,
          connection_name: :gnat,
          stream_name: "TEST_STREAM",
          consumer_name: "TEST_CONSUMER"
        }
      ]
      opts = [strategy: :one_for_one]
      Supervisor.start_link(children, opts)
    end
  end
  ```

  Note that the stream and its consumer, which names were given, must exist.
  """

  use Connection

  require Logger

  @doc """
  Called when the Pull Consumer recives a message. Depending on the value it returns, the acknowledgement
  is or is not sent.

  Possible return values explained:

  * `:ack` - acknowledges the message was handled and requests delivery of the next message to the reply
    subject.

  * `:nack` - signals that the message will not be processed now and processing can move onto the next
    message, NAK'd message will be retried.

  * `:noreply` - nothing is sent.
  """
  @callback handle_message(message :: Jetstream.message()) ::
              :ack | :nack | :noreply

  @type settings ::
          %{
            :connection_name => GenServer.server(),
            :stream_name => binary(),
            :consumer_name => binary(),
            optional(:connection_retry_timeout) => pos_integer(),
            optional(:connection_retries) => pos_integer()
          }
          | [
              connection_name: GenServer.server(),
              stream_name: binary(),
              consumer_name: binary(),
              connection_retry_timeout: pos_integer(),
              connection_retries: pos_integer()
            ]

  defmacro __using__(_opts) do
    quote do
      @behaviour Jetstream.PullConsumer

      @doc """
      Returns a specification to start this module under a supervisor.
      See `Supervisor`.
      """
      @spec child_spec(init_arg :: Jetstream.PullConsumer.settings()) :: Supervisor.child_spec()
      def child_spec(init_arg) do
        Jetstream.PullConsumer.child_spec(__MODULE__, init_arg)
      end

      @spec close(pull_consumer :: GenServer.server()) :: :ok
      def close(pull_consumer) do
        Jetstream.PullConsumer.close(pull_consumer)
      end

      defoverridable child_spec: 1,
                     close: 1
    end
  end

  @spec start_link(module :: module(), settings :: settings(), options :: GenServer.options()) ::
          GenServer.on_start()
  def start_link(module, settings, options \\ []) do
    state = %{
      settings: Map.new(settings),
      module: module
    }

    Connection.start_link(__MODULE__, state, options)
  end

  @spec child_spec(module :: module(), init_arg :: settings()) :: Supervisor.child_spec()
  def child_spec(module, init_arg) do
    %{
      id: __MODULE__,
      start:
        {__MODULE__, :start_link,
         [
           module,
           init_arg
         ]}
    }
  end

  @doc """
  Closes the NATS connection and stops the Pull Consumer.
  """
  @spec close(ref :: GenServer.server()) :: :ok
  def close(ref), do: Connection.call(ref, :close)

  def init(%{
        settings: settings,
        module: module
      }) do
    Process.flag(:trap_exit, true)

    initial_state = %{
      settings:
        settings
        |> Map.put_new(:connection_retry_timeout, 1_000)
        |> Map.put_new(:connection_retries, 10),
      listening_topic: "_CON.#{nuid()}",
      module: module
    }

    {:connect, :init, initial_state}
  end

  def connect(
        _,
        %{
          settings: %{
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
            #{__MODULE__} for #{stream_name}.#{consumer_name} failed to connect to NATS and retries limit \
            has been exhausted. Stopping.
            """,
            module: module,
            listening_topic: listening_topic,
            connection_name: connection_name
          )

          {:stop, :timeout, Map.delete(state, :current_retry)}
        else
          Logger.debug(
            """
            #{__MODULE__} for #{stream_name}.#{consumer_name} failed to connect to Gnat and will retry. \
            Reason: #{inspect(reason)}
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
          settings: %{
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
        "#{__MODULE__} for #{stream_name}.#{consumer_name} is shutting down",
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
    if Process.alive?(connection_name), do: {:ok, connection_name}, else: {:error, :not_alive}
  end

  defp connection_pid(connection_name) do
    case Process.whereis(connection_name) do
      nil ->
        {:error, :not_found}

      pid ->
        {:ok, pid}
    end
  end

  defp next_message(conn, stream_name, consumer_name, listening_topic) do
    Gnat.pub(
      conn,
      "$JS.API.CONSUMER.MSG.NEXT.#{stream_name}.#{consumer_name}",
      "1",
      reply_to: listening_topic
    )
  end

  def handle_info({:msg, message}, %{settings: settings, module: module} = state) do
    Logger.debug(
      """
      #{__MODULE__} for #{settings.stream_name}.#{settings.consumer_name} has received a message: \
      #{inspect(message, pretty: true)}
      """,
      module: module,
      listening_topic: state.listening_topic,
      subscription_id: state.subscription_id,
      connection_name: settings.connection_name
    )

    case module.handle_message(message) do
      :ack ->
        Jetstream.ack_next(message, state.listening_topic)

      :nack ->
        Jetstream.nack(message)

        next_message(
          message.gnat,
          settings.stream_name,
          settings.consumer_name,
          state.listening_topic
        )

      :noreply ->
        next_message(
          message.gnat,
          settings.stream_name,
          settings.consumer_name,
          state.listening_topic
        )
    end

    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, _reason}, state) do
    Logger.debug(
      """
        #{__MODULE__} for #{state.settings.stream_name}.#{state.settings.consumer_name}: NATS connection has died. \
        PullConsumer is reconnecting.
      """,
      module: state.module,
      listening_topic: state.listening_topic,
      subscription_id: state.subscription_id,
      connection_name: state.settings.connection_name
    )

    {:connect, :reconnect, state}
  end

  def handle_info(other, state) do
    Logger.debug(
      """
      #{__MODULE__} for #{state.settings.stream_name}.#{state.settings.consumer_name} received unexpected message: \
      #{inspect(other, pretty: true)}
      """,
      module: state.module,
      listening_topic: state.listening_topic,
      subscription_id: state.subscription_id,
      connection_name: state.settings.connection_name
    )

    {:noreply, state}
  end

  def handle_call(:close, from, state) do
    Logger.debug(
      "#{__MODULE__} for #{state.settings.stream_name}.#{state.settings.consumer_name} received a :close call.",
      module: state.module,
      listening_topic: state.listening_topic,
      subscription_id: state.subscription_id,
      connection_name: state.settings.connection_name
    )

    {:disconnect, {:close, from}, state}
  end

  defp nuid(), do: :crypto.strong_rand_bytes(12) |> Base.encode64()
end
