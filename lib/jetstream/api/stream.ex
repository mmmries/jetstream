defmodule Jetstream.API.Stream do
  import Jetstream.API.Util

  @enforce_keys [:name, :subjects]
  @derive Jason.Encoder
  defstruct [
    :description,
    :mirror,
    :name,
    :no_ack,
    :placement,
    :sources,
    :subjects,
    :template_owner,
    allow_rollup_hdrs: false,
    deny_delete: false,
    deny_purge: false,
    discard: :old,
    duplicate_window: 120_000_000_000,
    max_age: 0,
    max_bytes: -1,
    max_consumers: -1,
    max_msg_size: -1,
    max_msgs_per_subject: -1,
    max_msgs: -1,
    num_replicas: 1,
    retention: :limits,
    sealed: false,
    storage: :file
  ]

  @type nanoseconds :: non_neg_integer()

  @type stream_response :: %{
          cluster:
            nil
            | %{
                optional(:name) => binary(),
                optional(:leader) => binary(),
                optional(:replicas) =>
                  list(%{
                    :active => nanoseconds(),
                    :name => binary(),
                    :current => boolean(),
                    optional(:offline) => boolean(),
                    optional(:lag) => non_neg_integer()
                  })
              },
          config: t(),
          created: DateTime.t(),
          mirror: nil | stream_source(),
          sources: nil | list(stream_source()),
          state: stream_state()
        }

  @type stream_state :: %{
          bytes: non_neg_integer(),
          consumer_count: non_neg_integer(),
          deleted: nil | [non_neg_integer()],
          first_seq: non_neg_integer(),
          first_ts: DateTime.t(),
          last_seq: non_neg_integer(),
          last_ts: DateTime.t(),
          lost: nil | list(%{msgs: [non_neg_integer()], bytes: non_neg_integer()}),
          messages: non_neg_integer(),
          num_deleted: nil | integer(),
          num_subjects: nil | integer(),
          subjects:
            nil
            | %{}
            | %{
                binary() => non_neg_integer()
              }
        }

  @type streams :: %{
          limit: non_neg_integer(),
          offset: non_neg_integer(),
          streams: list(binary()),
          total: non_neg_integer()
        }

  @type stream_source :: %{
          :active => nanoseconds(),
          :lag => non_neg_integer(),
          :name => binary(),
          optional(:external) =>
            nil
            | %{
                api: binary(),
                deliver: binary()
              },
          optional(:error) =>
            nil
            | %{
                :code => integer(),
                optional(:err_code) => nil | non_neg_integer(),
                optional(:description) => nil | binary()
              }
        }

  @type t :: %__MODULE__{
          allow_rollup_hdrs: nil | boolean(),
          deny_delete: boolean(),
          deny_purge: boolean(),
          description: nil | binary(),
          discard: :old | :new,
          duplicate_window: nil | nanoseconds(),
          max_age: nanoseconds(),
          max_bytes: integer(),
          max_consumers: integer(),
          max_msg_size: nil | integer(),
          max_msgs: integer(),
          mirror: nil | stream_source(),
          name: binary(),
          no_ack: nil | boolean(),
          num_replicas: pos_integer(),
          placement:
            nil
            | %{
                :cluster => binary(),
                optional(:tags) => list(binary())
              },
          retention: :limits | :workqueue | :interest,
          sealed: boolean(),
          sources: nil | list(stream_source()),
          storage: :file | :memory,
          subjects: nil | list(binary()),
          template_owner: nil | binary()
        }

  @doc "create a new stream, please see https://github.com/nats-io/jetstream#streams for details on supported arguments"
  @spec create(Gnat.t(), t()) :: {:ok, stream_response()} | {:error, any()}
  def create(conn, %__MODULE__{} = stream) do
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
      deleted: Map.get(state, "deleted"),
      first_seq: Map.fetch!(state, "first_seq"),
      first_ts: Map.fetch!(state, "first_ts") |> to_datetime(),
      last_seq: Map.fetch!(state, "last_seq"),
      last_ts: Map.fetch!(state, "last_ts") |> to_datetime(),
      lost: Map.get(state, "lost"),
      messages: Map.fetch!(state, "messages"),
      num_deleted: Map.get(state, "num_deleted"),
      num_subjects: Map.get(state, "num_subjects"),
      subjects: Map.get(state, "subjects")
    }
  end

  defp to_stream(stream) do
    %__MODULE__{
      allow_rollup_hdrs: Map.get(stream, "allow_rollup_hdrs"),
      deny_delete: Map.fetch!(stream, "deny_delete"),
      deny_purge: Map.fetch!(stream, "deny_purge"),
      description: Map.get(stream, "description"),
      discard: Map.fetch!(stream, "discard") |> to_sym(),
      duplicate_window: Map.get(stream, "duplicate_window"),
      max_age: Map.fetch!(stream, "max_age"),
      max_bytes: Map.fetch!(stream, "max_bytes"),
      max_consumers: Map.fetch!(stream, "max_consumers"),
      max_msg_size: Map.get(stream, "max_msg_size"),
      max_msgs: Map.fetch!(stream, "max_msgs"),
      mirror: Map.get(stream, "mirror"),
      name: Map.fetch!(stream, "name"),
      no_ack: Map.get(stream, "no_ack"),
      num_replicas: Map.fetch!(stream, "num_replicas"),
      placement: Map.get(stream, "placement"),
      retention: Map.fetch!(stream, "retention") |> to_sym(),
      sealed: Map.fetch!(stream, "sealed"),
      sources: Map.get(stream, "sources"),
      storage: Map.fetch!(stream, "storage") |> to_sym(),
      subjects: Map.get(stream, "subjects"),
      template_owner: Map.get(stream, "template_owner")
    }
  end

  defp to_stream_response(
         %{"config" => config, "state" => state, "created" => created} = response
       ) do
    with {:ok, created, _} <- DateTime.from_iso8601(created) do
      %{
        cluster: Map.get(response, "cluster"),
        config: to_stream(config),
        created: created,
        mirror: Map.get(response, "mirror"),
        sources: Map.get(response, "sources"),
        state: to_state(state)
      }
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
