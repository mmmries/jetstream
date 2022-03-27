defmodule Jetstream.API.Stream do
  @moduledoc """
  A module representing a NATS JetStream Stream.

  Learn more about Streams: https://docs.nats.io/nats-concepts/jetstream/streams

  ## The Jetstream.API.Stream struct

  The struct's mandatory fields are `:name` and `:subjects`. The rest will have the NATS
  default values set.

  Stream struct fields explanation:

  * `:allow_rollup_hdrs` - allows the use of the Nats-Rollup header to replace all contents of a stream,
    or subject in a stream, with a single new message.
  * `:deny_delete` - restricts the ability to delete messages from a stream via the API. Cannot be changed
    once set to true.
  * `:deny_purge` - restricts the ability to purge messages from a stream via the API. Cannot be change
    once set to true.
  * `:description` - a short description of the purpose of this stream.
  * `:discard` - determines what happens when a Stream reaches its limits. It has the following options:
     - `:old` - the default option. Old messages are deleted.
     - `:new` - refuses new messages.
  * `:duplicate_window` - the window within which to track duplicate messages, expressed in nanoseconds.
  * `:max_age` - maximum age of any message in the Stream, expressed in nanoseconds.
  * `:max_bytes` - how many bytes the Stream may contain. Adheres to `:discard`, removing oldest or
    refusing new messages if the Stream exceeds this size.
  * `:max_consumers` - how many Consumers can be defined for a given Stream, -1 for unlimited.
  * `:max_msg_size` - the largest message that will be accepted by the Stream.
  * `:max_msgs` - how many messages may be in a Stream. Adheres to `:discard`, removing oldest or refusing
    new messages if the Stream exceeds this number of messages
  * `:mirror` - maintains a 1:1 mirror of another stream with name matching this property.  When a mirror
    is configured subjects and sources must be empty.
  * `:name` - a name for the Stream.
    See [naming](https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/naming).
  * `:no_ack` - disables acknowledging messages that are received by the Stream.
  * `:num_replicas` - how many replicas to keep for each message.
  * `:placement` - placement directives to consider when placing replicas of this stream, random placement
    when unset. It has the following properties:
     - `:cluster` - the desired cluster name to place the stream.
     - `:tags` - tags required on servers hosting this stream.
  * `:retention` - how messages are retained in the Stream. Once this is exceeded, old messages are removed.
    It has the following options:
     - `:limits` - the default policy.
     - `:interest`
     - `:workqueue`
  * `:sealed` - sealed streams do not allow messages to be deleted via limits or API, sealed streams can not
    be unsealed via configuration update. Can only be set on already created streams via the Update API.
  * `:sources` - list of stream names to replicate into this stream.
  * `:storage` - the type of storage backend. Available options:
     - `:file`
     - `:memory`
  * `:subjects` - a list of subjects to consume, supports wildcards.
  * `:template_owner` - when the Stream is managed by a Stream Template this identifies the template that
    manages the Stream.
  """

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

  @type t :: %__MODULE__{
          allow_rollup_hdrs: boolean(),
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
          mirror: nil | source(),
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
          sources: nil | list(source()),
          storage: :file | :memory,
          subjects: nil | list(binary()),
          template_owner: nil | binary()
        }

  @typedoc """
  Stream source fields explained:

  * `:name` - stream name.
  * `:opt_start_seq` - sequence to start replicating from.
  * `:opt_start_time` - timestamp to start replicating from.
  * `:filter_subject` - replicate only a subset of messages based on filter.
  * `:external` - configuration referencing a stream source in another account or JetStream domain.
    It has the following parameters:
     - `:api` - the subject prefix that imports other account/domain `$JS.API.CONSUMER.>` subjects
     - `:deliver` - the delivery subject to use for push consumer
  """
  @type source :: %{
          :name => binary(),
          optional(:opt_start_seq) => integer(),
          optional(:opt_start_time) => DateTime.t(),
          optional(:filter_subject) => binary(),
          optional(:external) => %{
            api: binary(),
            deliver: binary()
          }
        }

  @type info :: %{
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
          mirror: nil | source_info(),
          sources: nil | list(source_info()),
          state: state()
        }

  @type state :: %{
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

  @type source_info :: %{
          :active => nanoseconds(),
          :lag => non_neg_integer(),
          :name => binary(),
          optional(:external) => %{
            api: binary(),
            deliver: binary()
          },
          optional(:error) => %{
            :code => integer(),
            optional(:err_code) => nil | non_neg_integer(),
            optional(:description) => nil | binary()
          }
        }

  @type streams :: %{
          limit: non_neg_integer(),
          offset: non_neg_integer(),
          streams: list(binary()),
          total: non_neg_integer()
        }

  @doc """
  Creates a new Stream.

  ## Examples

      iex> {:ok, %{created: _}} = Jetstream.API.Stream.create(:gnat, %Jetstream.API.Stream{name: "stream", subjects: ["subject"]})

  """
  @spec create(conn :: Gnat.t(), stream :: t()) :: {:ok, info()} | {:error, any()}
  def create(conn, %__MODULE__{} = stream) do
    with :ok <- validate(stream),
         {:ok, stream} <-
           request(conn, "$JS.API.STREAM.CREATE.#{stream.name}", Jason.encode!(stream)) do
      {:ok, to_info(stream)}
    end
  end

  @doc """
  Deletes a Stream and all its data.

  ## Examples

      iex> Jetstream.API.Stream.create(:gnat, %Jetstream.API.Stream{name: "stream", subjects: ["subject"]})
      iex> Jetstream.API.Stream.delete(:gnat, "stream")
      :ok

      iex> {:error, %{"code" => 404, "description" => "stream not found"}} = Jetstream.API.Stream.delete(:gnat, "wrong_stream")

  """
  @spec delete(conn :: Gnat.t(), stream_name :: binary()) :: :ok | {:error, any()}
  def delete(conn, stream_name) when is_binary(stream_name) do
    with {:ok, _response} <- request(conn, "$JS.API.STREAM.DELETE.#{stream_name}", "") do
      :ok
    end
  end

  @doc """
  Information about config and state of a Stream.

  ## Examples

      iex> Jetstream.API.Stream.create(:gnat, %Jetstream.API.Stream{name: "stream", subjects: ["subject"]})
      iex> {:ok, %{created: _}} = Jetstream.API.Stream.info(:gnat, "stream")

      iex> {:error, %{"code" => 404, "description" => "stream not found"}} = Jetstream.API.Stream.info(:gnat, "wrong_stream")

  """
  @spec info(conn :: Gnat.t(), stream_name :: binary()) ::
          {:ok, info()} | {:error, any()}
  def info(conn, stream_name) when is_binary(stream_name) do
    with {:ok, decoded} <- request(conn, "$JS.API.STREAM.INFO.#{stream_name}", "") do
      {:ok, to_info(decoded)}
    end
  end

  @doc """
  Paged list of known Streams including all their current information.

  ## Examples

      iex> {:ok, %{total: _, offset: 0, limit: 1024, streams: _}} = Jetstream.API.Stream.list(:gnat)

  """
  @spec list(conn :: Gnat.t(), params :: [offset: non_neg_integer()]) ::
          {:ok, streams()} | {:error, term()}
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
      sources: Map.get(stream, "sources"),
      storage: Map.fetch!(stream, "storage") |> to_sym(),
      subjects: Map.get(stream, "subjects"),
      template_owner: Map.get(stream, "template_owner")
    }
    # Check for fields added in NATS versions higher than 2.2.0
    |> put_if_exist(:allow_rollup_hdrs, stream, "allow_rollup_hdrs")
    |> put_if_exist(:deny_delete, stream, "deny_delete")
    |> put_if_exist(:deny_purge, stream, "deny_purge")
    |> put_if_exist(:sealed, stream, "sealed")
  end

  defp to_info(%{"config" => config, "state" => state, "created" => created} = response) do
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

      String.contains?(stream_settings.name, [".", "*", ">", " ", "\t"]) ->
        {:error, "invalid stream name, cannot contain '.', '>', '*', spaces or tabs"}

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
