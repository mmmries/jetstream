defmodule Jetstream.API.Stream do
  import Jetstream.API.Util

  @enforce_keys [:name, :subjects]
  @derive Jason.Encoder
  defstruct name: nil,
            subjects: [],
            max_age: 0,
            max_bytes: -1,
            max_msg_size: -1,
            max_msgs: -1,
            max_consumers: -1,
            retention: :limits,
            discard: :old,
            duplicate_window: 120_000_000_000,
            storage: :file,
            num_replicas: 1

  @type nanoseconds :: non_neg_integer()

  @type stream_response :: %{
    state: stream_state(),
    config: t(),
    created: DateTime.t()
  }

  @type stream_state :: %{
    bytes: non_neg_integer(),
    consumer_count: non_neg_integer(),
    first_seq: non_neg_integer(),
    first_ts: DateTime.t(),
    last_seq: non_neg_integer(),
    last_ts: DateTime.t(),
    messages: non_neg_integer()
  }

  @type streams :: %{
    limit: non_neg_integer(),
    offset: non_neg_integer(),
    streams: list(binary()),
    total: non_neg_integer()
  }

  @type t :: %__MODULE__{
    name: binary(),
    subjects: list(binary()),
    max_age: nanoseconds(),
    max_bytes: integer(),
    max_msg_size: integer(),
    max_msgs: integer(),
    max_consumers: integer(),
    retention: :limits | :workqueue | :interest,
    discard: :old | :new,
    duplicate_window: nanoseconds(),
    storage: :file | :memory,
    num_replicas: pos_integer()
  }

  @doc "create a new stream, please see https://github.com/nats-io/jetstream#streams for details on supported arguments"
  @spec create(Gnat.t(), t()) :: {:ok, stream_response()} | {:error, any()}
  def create(conn, %__MODULE__{}=stream) do
    with :ok <- validate(stream),
         {:ok, stream} <-
           request(conn, "$JS.API.STREAM.CREATE.#{stream.name}", Jason.encode!(stream)) do
      {:ok, to_stream_response(stream)}
    end
  end

  @spec delete(Gnat.t(), binary()) :: :ok | {:error, any()}
  def delete(conn, stream_name) when is_binary(stream_name) do
    with {:ok, _response} <- request(conn, "$JS.API.STREAM.DELETE.#{stream_name}", "") do
      :ok
    end
  end

  @spec info(Gnat.t(), binary()) :: {:ok, stream_response()} | {:error, any()}
  def info(conn, stream_name) when is_binary(stream_name) do
    with {:ok, decoded} <- request(conn, "$JS.API.STREAM.INFO.#{stream_name}", "") do
      {:ok, to_stream_response(decoded)}
    end
  end

  @spec list(Gnat.t(), offset: non_neg_integer()) :: {:ok, streams()} | {:error, term()}
  def list(conn, params \\ []) do
    payload =
      Jason.encode!(%{
        offset: Keyword.get(params, :offset, 0)
      })

    with {:ok, decoded} <- request(conn, "$JS.API.STREAM.NAMES", payload) do
      result = %{
        limit: Map.get(decoded, "limit"),
        offset: Map.get(decoded, "offset"),
        streams: Map.get(decoded, "streams"),
        total: Map.get(decoded, "total")
      }

      {:ok, result}
    end
  end

  defp to_state(state) do
    %{
      bytes: Map.fetch!(state, "bytes"),
      consumer_count: Map.fetch!(state, "consumer_count"),
      first_seq: Map.fetch!(state, "first_seq"),
      first_ts: Map.fetch!(state, "first_ts") |> to_datetime(),
      last_seq: Map.fetch!(state, "last_seq"),
      last_ts: Map.fetch!(state, "last_ts") |> to_datetime(),
      messages: Map.fetch!(state, "messages")
    }
  end

  defp to_stream(stream) do
    %__MODULE__{
      name: Map.fetch!(stream, "name"),
      subjects: Map.fetch!(stream, "subjects"),
      max_age: Map.fetch!(stream, "max_age"),
      max_bytes: Map.fetch!(stream, "max_bytes"),
      max_msg_size: Map.fetch!(stream, "max_msg_size"),
      max_msgs: Map.fetch!(stream, "max_msgs"),
      max_consumers: Map.fetch!(stream, "max_consumers"),
      retention: Map.fetch!(stream, "retention") |> String.to_existing_atom(),
      discard: Map.fetch!(stream, "discard") |> String.to_existing_atom(),
      duplicate_window: Map.fetch!(stream, "duplicate_window"),
      storage: Map.fetch!(stream, "storage") |> String.to_existing_atom(),
      num_replicas: Map.fetch!(stream, "num_replicas")
    }
  end

  defp to_stream_response(%{"config" => config, "state" => state}) do
    %{
      config: to_stream(config),
      state: to_state(state)
    }
  end

  defp validate(stream_settings) do
    cond do
      Map.has_key?(stream_settings, :name) == false ->
        {:error, "Must have a :name set"}

      is_binary(Map.get(stream_settings, :name)) == false ->
        {:error, "name must be a string"}

      Map.has_key?(stream_settings, :subjects) == false ->
        {:error, "You must specify a :subjects key"}

      is_list(Map.get(stream_settings, :subjects)) == false ->
        {:error, ":subjects must be a list of strings"}

      Enum.all?(Map.get(stream_settings, :subjects), &is_binary/1) == false ->
        {:error, ":subjects must be a list of strings"}

      true ->
        :ok
    end
  end
end
