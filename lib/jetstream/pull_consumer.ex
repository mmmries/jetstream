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

  You can then place your Pull Consumer in a supervision tree. Remember that you need to have a Gnat
  conection set up.

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
            stream_name: "TEST",
            consumer_name: "TEST"
          }}
      ]
      opts = [strategy: :one_for_one]
      Supervisor.start_link(children, opts)
    end
  end
  ```
  """

  use GenServer

  @doc """
  Called when the Pull Consumer recives a message. Depending on the value it returns, the acknowledgement
  is or is not sent.

  Possible return values explained:

  * `:ack`: acknowledges the message was handled and requests delivery of the next message to the reply
    subject.

  * `:nack`: signals that the message will not be processed now and processing can move onto the next
    message, NAK'd message will be retried.

  * `:noreply`: nothing is sent.

  The client should be listening for acknowledgements on the `$JS.ACK.<stream>.<consumer>.>` subject.
  """
  @callback handle_message(message :: Jetstream.message()) ::
              :ack | :nack | :noreply

  @type settings :: %{
          connection_name: pid() | atom(),
          stream_name: binary(),
          consumer_name: binary(),
          module: atom()
        }

  defmacro __using__(_opts) do
    quote do
      @behaviour Jetstream.PullConsumer

      @type settings :: %{
              connection_name: pid() | atom(),
              stream_name: binary(),
              consumer_name: binary()
            }

      @spec init(init_arg :: settings()) :: {:ok, Jetstream.PullConsumer.settings()}
      def init(init_arg) do
        init_arg
        |> Map.put(:module, __MODULE__)
        |> Jetstream.PullConsumer.init()
      end

      @spec child_spec(init_arg :: settings()) :: Jetstream.PullConsumer.settings()
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

  @spec start_link(settings(), GenServer.options()) :: GenServer.on_start()
  def start_link(settings, options \\ []) do
    GenServer.start_link(__MODULE__, settings, options)
  end

  def init(%{
        connection_name: connection_name,
        stream_name: stream_name,
        consumer_name: consumer_name,
        module: module
      }) do
    if is_pid(connection_name),
      do: Process.link(connection_name),
      else: Process.whereis(connection_name) |> Process.link()

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
    case state.module.handle_message(message) do
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
