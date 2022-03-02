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
          %{
            connection_name: :gnat,
            stream_name: "TEST_STREAM",
            consumer_name: "TEST_CONSUMER"
          }}
      ]
      opts = [strategy: :one_for_one]
      Supervisor.start_link(children, opts)
    end
  end
  ```

  Note that the stream and its consumer, which names were given, must exist.
  """

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

  @type settings :: %{
          connection_name: pid() | atom(),
          stream_name: binary(),
          consumer_name: binary()
        }

  defmacro __using__(_opts) do
    quote do
      @behaviour Jetstream.PullConsumer

      @doc """
      Returns a specification to start this module under a supervisor.
      See `Supervisor`.
      """
      @spec child_spec(
              init_arg :: %{
                connection_name: pid() | atom(),
                stream_name: binary(),
                consumer_name: binary()
              }
            ) :: Supervisor.child_spec()
      def child_spec(init_arg) do
        Jetstream.PullConsumer.child_spec(__MODULE__, init_arg)
      end

      defoverridable child_spec: 1
    end
  end

  @spec start_link(module :: module(), settings :: settings(), options :: GenServer.options()) ::
          GenServer.on_start()
  def start_link(module, settings, options \\ []) do
    settings = %{
      settings: settings,
      module: module
    }

    GenServer.start_link(__MODULE__, settings, options)
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

  def init(arg) do
    {:ok, arg, {:continue, :connect}}
  end

  def handle_continue(:connect, %{
        settings: %{
          connection_name: connection_name,
          stream_name: stream_name,
          consumer_name: consumer_name
        },
        module: module
      }) do
    with {:ok, conn} <- connection_pid(connection_name),
         Process.link(conn),
         listening_topic = "_CON.#{nuid()}",
         {:ok, _sid} <- Gnat.sub(conn, self(), listening_topic),
         :ok <- next_message(conn, stream_name, consumer_name, listening_topic) do
      state = %{
        settings: %{
          stream_name: stream_name,
          consumer_name: consumer_name,
          listening_topic: listening_topic
        },
        module: module
      }

      {:noreply, state}
    else
      error -> {:stop, error, %{}}
    end
  end

  defp connection_pid(connection_name, retries \\ 5)

  defp connection_pid(_connection_name, 0), do: {:error, :not_found}

  defp connection_pid(connection_name, _retries) when is_pid(connection_name),
    do: {:ok, connection_name}

  defp connection_pid(connection_name, retries) do
    case Process.whereis(connection_name) do
      nil ->
        # WORKAROUND: Gnat connection is not started immediately with Connection Supervisor.
        # This is a temporary hack.
        Process.sleep(500)
        connection_pid(connection_name, retries - 1)

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
    case module.handle_message(message) do
      :ack ->
        Jetstream.ack_next(message, settings.listening_topic)

      :nack ->
        Jetstream.nack(message)

        next_message(
          message.gnat,
          settings.stream_name,
          settings.consumer_name,
          settings.listening_topic
        )

      :noreply ->
        next_message(
          message.gnat,
          settings.stream_name,
          settings.consumer_name,
          settings.listening_topic
        )
    end

    {:noreply, state}
  end

  def handle_info(other, state) do
    require Logger

    Logger.error("""
    #{__MODULE__} for #{state.settings.stream_name}.#{state.settings.consumer_name} received unexpected message:
    #{inspect(other)}
    """)

    {:noreply, state}
  end

  defp nuid(), do: :crypto.strong_rand_bytes(12) |> Base.encode64()
end
