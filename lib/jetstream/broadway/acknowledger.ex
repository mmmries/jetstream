with {:module, _} <- Code.ensure_compiled(Broadway) do
  defmodule Jetstream.Broadway.Acknowledger do
    @moduledoc false
    alias Broadway.Acknowledger

    @behaviour Acknowledger

    @type ack_date :: %{
            optional(:on_failure) => ack_option,
            optional(:on_success) => ack_option
          }

    @type ack_option :: :ack | :nack

    @type ack_ref :: reference()

    @type t :: %__MODULE__{
            connection_name: String.t(),
            on_failure: ack_option,
            on_success: ack_option
          }

    @enforce_keys [:connection_name]
    defstruct [:connection_name, :on_failure, :on_success]

    def init(opts) do
      with {:ok, on_success} <- validate(opts, :on_success, :ack),
           {:ok, on_failure} <- validate(opts, :on_failure, :nack) do
        state = %__MODULE__{
          connection_name: opts[:connection_name],
          on_success: on_success,
          on_failure: on_failure
        }

        ack_ref = make_ref()
        put_config(ack_ref, state)

        {:ok, ack_ref}
      end
    end

    defp put_config(reference, state) do
      :persistent_term.put({__MODULE__, reference}, state)
    end

    def get_config(reference) do
      :persistent_term.get({__MODULE__, reference})
    end

    @impl Acknowledger
    def ack(ack_ref, successful, failed) do
      config = get_config(ack_ref)

      IO.inspect(config)

      IO.inspect(successful)
      IO.inspect(failed)

      # TODO: Acknowledge these

      :ok
    end

    @impl Acknowledger
    def configure(_ack_ref, ack_data, options) do
      options = assert_valid_config!(options)
      ack_data = Map.merge(ack_data, Map.new(options))
      {:ok, ack_data}
    end

    defp assert_valid_config!(options) do
      Enum.map(options, fn
        {:on_success, value} -> {:on_success, validate_option!(:on_success, value)}
        {:on_failure, value} -> {:on_failure, validate_option!(:on_failure, value)}
        {other, _value} -> raise ArgumentError, "unsupported configure option #{inspect(other)}"
      end)
    end

    defp validate(opts, key, default) when is_list(opts) do
      validate_option(key, opts[key] || default)
    end

    defp validate_option(action, value) when action in [:on_success, :on_failure] do
      case validate_action(value) do
        {:ok, result} -> {:ok, result}
        :error -> validation_error(action, "a valid acknowledgement option", value)
      end
    end

    defp validate_option(_, value), do: {:ok, value}

    defp validate_option!(key, value) do
      case validate_option(key, value) do
        {:ok, value} -> value
        {:error, message} -> raise ArgumentError, message
      end
    end

    defp validation_error(option, expected, value) do
      {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
    end

    defp validate_action(:ack), do: {:ok, :ack}
    defp validate_action(:nack), do: {:ok, :nack}
    defp validate_action(_), do: :error
  end
end
