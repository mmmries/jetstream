defmodule Jetstream.Stream do
  @type streams :: %{
          limit: non_neg_integer(),
          offset: non_neg_integer(),
          streams: list(binary()),
          total: non_neg_integer()
        }

  @defaults %{
    max_age: -1,
    max_bytes: -1,
    max_msg_size: -1,
    max_msgs: -1,
    max_consumers: -1,
    retention: "limits",
    discard: "old",
    storage: "file",
    num_replicas: 1
  }

  @doc "create a new stream, please see https://github.com/nats-io/jetstream#streams for details on supported arguments"
  def create(conn, settings) do
    payload = Map.merge(@defaults, settings)

    with :ok <- validate(payload),
         {:ok, stream} <-
           request(conn, "$JS.API.STREAM.CREATE.#{payload.name}", Jason.encode!(payload)) do
      {:ok, stream}
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

  defp request(conn, topic, payload) do
    with {:ok, %{body: body}} <- Gnat.request(conn, topic, payload),
         {:ok, decoded} <- Jason.decode(body) do
      case decoded do
        %{"error" => err} ->
          {:error, err}

        other ->
          {:ok, other}
      end
    end
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
