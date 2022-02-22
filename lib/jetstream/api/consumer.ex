defmodule Jetstream.API.Consumer do
  import Jetstream.API.Util

  @enforce_keys [:stream_name]
  defstruct [
    :backoff,
    :deliver_group,
    :deliver_subject,
    :description,
    :durable_name,
    :filter_subject,
    :flow_control,
    :headers_only,
    :idle_heartbeat,
    :inactive_threshold,
    :max_batch,
    :max_expires,
    :max_waiting,
    :opt_start_seq,
    :opt_start_time,
    :rate_limit_bps,
    :sample_freq,
    :stream_name,
    ack_policy: :explicit,
    ack_wait: 30_000_000_000,
    deliver_policy: :all,
    max_ack_pending: 20_000,
    max_deliver: -1,
    replay_policy: :instant
  ]

  @type consumer_response :: %{
          ack_floor: %{
            consumer_seq: non_neg_integer(),
            stream_seq: non_neg_integer()
          },
          cluster:
            nil
            | %{
                optional(:name) => binary(),
                optional(:leader) => binary(),
                optional(:replicas) => [
                  %{
                    :active => non_neg_integer(),
                    :current => boolean(),
                    :name => binary(),
                    optional(:lag) => non_neg_integer(),
                    optional(:offline) => boolean()
                  }
                ]
              },
          config: consumer_config(),
          created: DateTime.t(),
          delivered: %{
            consumer_seq: non_neg_integer(),
            stream_seq: non_neg_integer()
          },
          name: binary(),
          num_ack_pending: non_neg_integer(),
          num_pending: non_neg_integer(),
          num_redelivered: non_neg_integer(),
          num_waiting: non_neg_integer(),
          push_bound: nil | boolean(),
          stream_name: binary()
        }

  @type consumer_config :: %{
          ack_policy: :none | :all | :explicit,
          ack_wait: nil | non_neg_integer(),
          backoff: nil | [non_neg_integer()],
          deliver_group: nil | binary(),
          deliver_policy: :all | :last | :new | :by_start_sequence | :by_start_time,
          deliver_subject: nil | binary(),
          description: nil | binary(),
          durable_name: nil | binary(),
          filter_subject: nil | binary(),
          flow_control: nil | boolean(),
          headers_only: nil | boolean(),
          idle_heartbeat: nil | non_neg_integer(),
          inactive_threshold: nil | non_neg_integer(),
          max_ack_pending: nil | integer(),
          max_batch: nil | integer(),
          max_deliver: nil | integer(),
          max_expires: nil | non_neg_integer(),
          max_waiting: nil | integer(),
          opt_start_seq: nil | non_neg_integer(),
          opt_start_time: nil | DateTime.t(),
          rate_limit_bps: nil | non_neg_integer(),
          replay_policy: :instant | :original,
          sample_freq: nil | binary()
        }

  @type consumers :: %{
          consumers: list(binary()),
          limit: non_neg_integer(),
          offset: non_neg_integer(),
          total: non_neg_integer()
        }

  @type t ::
          Map.merge(
            %__MODULE__{
              stream_name: binary()
            },
            consumer_config()
          )

  @spec create(Gnat.t(), t()) :: {:ok, consumer_response()} | {:error, term()}
  def create(gnat, %__MODULE__{durable_name: name} = consumer) when not is_nil(name) do
    create_topic = "$JS.API.CONSUMER.DURABLE.CREATE.#{consumer.stream_name}.#{name}"

    with {:ok, raw_response} <- request(gnat, create_topic, create_payload(consumer)) do
      {:ok, to_consumer_response(raw_response)}
    end
  end

  def create(gnat, %__MODULE__{} = consumer) do
    create_topic = "$JS.API.CONSUMER.CREATE.#{consumer.stream_name}"

    with {:ok, raw_response} <- request(gnat, create_topic, create_payload(consumer)) do
      {:ok, to_consumer_response(raw_response)}
    end
  end

  @spec delete(Gnat.t(), binary(), binary()) :: :ok | {:error, any()}
  def delete(gnat, stream_name, consumer_name) do
    topic = "$JS.API.CONSUMER.DELETE.#{stream_name}.#{consumer_name}"

    with {:ok, _response} <- request(gnat, topic, "") do
      :ok
    end
  end

  @spec info(Gnat.t(), binary(), binary()) :: {:ok, consumer_response()} | {:error, any()}
  def info(gnat, stream_name, consumer_name) do
    topic = "$JS.API.CONSUMER.INFO.#{stream_name}.#{consumer_name}"

    with {:ok, raw} <- request(gnat, topic, "") do
      {:ok, to_consumer_response(raw)}
    end
  end

  @spec list(Gnat.t(), binary(), offset: non_neg_integer()) ::
          {:ok, consumers()} | {:error, term()}
  def list(gnat, stream_name, params \\ []) do
    payload =
      Jason.encode!(%{
        offset: Keyword.get(params, :offset, 0)
      })

    with {:ok, raw} <- request(gnat, "$JS.API.CONSUMER.NAMES.#{stream_name}", payload) do
      response = %{
        consumers: Map.get(raw, "consumers"),
        limit: Map.get(raw, "limit"),
        offset: Map.get(raw, "offset"),
        total: Map.get(raw, "total")
      }

      {:ok, response}
    end
  end

  defp create_payload(%__MODULE__{} = cons) do
    %{
      config: %{
        ack_policy: cons.ack_policy,
        ack_wait: cons.ack_wait,
        backoff: cons.backoff,
        deliver_group: cons.deliver_group,
        deliver_policy: cons.deliver_policy,
        deliver_subject: cons.deliver_subject,
        description: cons.description,
        durable_name: cons.durable_name,
        filter_subject: cons.filter_subject,
        flow_control: cons.flow_control,
        headers_only: cons.headers_only,
        idle_heartbeat: cons.idle_heartbeat,
        inactive_threshold: cons.inactive_threshold,
        max_ack_pending: cons.max_ack_pending,
        max_batch: cons.max_batch,
        max_deliver: cons.max_deliver,
        max_expires: cons.max_expires,
        max_waiting: cons.max_waiting,
        opt_start_seq: cons.opt_start_seq,
        opt_start_time: cons.opt_start_time,
        rate_limit_bps: cons.rate_limit_bps,
        replay_policy: cons.replay_policy,
        sample_freq: cons.sample_freq
      },
      stream_name: cons.stream_name
    }
    |> Jason.encode!()
  end

  defp to_consumer_config(raw) do
    %{
      ack_policy: raw |> Map.get("ack_policy") |> to_sym(),
      ack_wait: raw |> Map.get("ack_wait"),
      backoff: Map.get(raw, "backoff"),
      deliver_group: Map.get(raw, "deliver_group"),
      deliver_policy: raw |> Map.get("deliver_policy") |> to_sym(),
      deliver_subject: raw |> Map.get("deliver_subject"),
      description: Map.get(raw, "description"),
      durable_name: Map.get(raw, "durable_name"),
      filter_subject: raw |> Map.get("filter_subject"),
      flow_control: Map.get(raw, "flow_control"),
      headers_only: Map.get(raw, "headers_only"),
      idle_heartbeat: Map.get(raw, "idle_heartbeat"),
      inactive_threshold: Map.get(raw, "inactive_threshold"),
      max_ack_panding: Map.get(raw, "max_ack_pending"),
      max_batch: Map.get(raw, "max_batch"),
      max_deliver: Map.get(raw, "max_deliver"),
      max_expires: Map.get(raw, "max_expires"),
      max_waiting: Map.get(raw, "max_waiting"),
      opt_start_seq: raw |> Map.get("opt_start_seq"),
      opt_start_time: raw |> Map.get("opt_start_time") |> to_datetime(),
      rate_limit_bps: Map.get(raw, "rate_limit_bps"),
      replay_policy: raw |> Map.get("replay_policy") |> to_sym(),
      sample_freq: Map.get(raw, "sample_freq")
    }
  end

  defp to_consumer_response(raw) do
    %{
      ack_floor: %{
        consumer_seq: get_in(raw, ["ack_floor", "consumer_seq"]),
        stream_seq: get_in(raw, ["ack_floor", "stream_seq"])
      },
      cluster: Map.get(raw, "cluster"),
      config: to_consumer_config(Map.get(raw, "config")),
      created: raw |> Map.get("created") |> to_datetime(),
      delivered: %{
        consumer_seq: get_in(raw, ["delivered", "consumer_seq"]),
        stream_seq: get_in(raw, ["delivered", "stream_seq"])
      },
      name: Map.get(raw, "name"),
      num_ack_pending: Map.get(raw, "num_ack_pending"),
      num_pending: Map.get(raw, "num_pending"),
      num_redelivered: Map.get(raw, "num_redelivered"),
      num_waiting: Map.get(raw, "num_waiting"),
      push_bound: Map.get(raw, "push_bound"),
      stream_name: Map.get(raw, "stream_name")
    }
  end
end
