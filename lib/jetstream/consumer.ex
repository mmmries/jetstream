defmodule Jetstream.Consumer do
  import Jetstream.Util

  @enforce_keys [:stream_name, :name]
  defstruct stream_name: nil,
            name: nil,
            deliver_subject: nil,
            deliver_policy: :all,
            durable: true,
            ack_policy: :explicit,
            ack_wait: 30_000_000_000, # 30sec
            max_deliver: -1,
            replay_policy: :instant,
            opt_start_time: nil,
            opt_start_seq: nil

  @type consumer_response :: %{
    stream_name: binary(),
    name: binary(),
    created: DateTime.t(),
    config: consumer_config(),
    delivered: %{
      consumer_seq: non_neg_integer(),
      stream_seq: non_neg_integer()
    },
    ack_floor: %{
      consumer_seq: non_neg_integer(),
      stream_seq: non_neg_integer()
    },
    num_pending: non_neg_integer(),
    num_redelivered: non_neg_integer()
  }

  @type consumer_config :: %{
    durable_name: binary(),
    deliver_policy: :all | :last | :new | :by_start_sequence | :by_start_time,
    deliver_subject: nil | binary(),
    ack_wait: nil | non_neg_integer(),
    ack_policy: :none | :all | :explicit,
    replay_policy: :instant | :original,
    filter_subject: nil | binary(),
    opt_start_time: nil | DateTime.t(),
    opt_start_seq: nil | non_neg_integer()
  }

  @type consumers :: %{
    consumers: list(binary()),
    limit: non_neg_integer(),
    offset: non_neg_integer(),
    total: non_neg_integer()
  }

  @type t :: %__MODULE__{
    stream_name: binary(),
    name: binary(),
  }

  @spec create(Gnat.t(), t()) :: {:ok, consumer_response()} | {:error, term()}
  def create(gnat, %__MODULE__{durable: true} = consumer) do
    create_topic = "$JS.API.CONSUMER.DURABLE.CREATE.#{consumer.stream_name}.#{consumer.name}"
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

  @spec list(Gnat.t(), binary(), offset: non_neg_integer()) :: {:ok, consumers()} | {:error, term()}
  def list(gnat, stream_name, params \\ []) do
    payload = Jason.encode!(%{
      offset: Keyword.get(params, :offset, 0)
    })

    with {:ok, raw} <- request(gnat, "$JS.API.CONSUMER.NAMES.#{stream_name}", payload) do
      response = %{
        consumers: Map.get(raw, "consumers"),
        offset: Map.get(raw, "offset"),
        limit: Map.get(raw, "limit"),
        total: Map.get(raw, "total")
      }
      {:ok, response}
    end
  end

  defp create_payload(%__MODULE__{} = cons) do
    %{
      stream_name: cons.stream_name,
      config: %{
        durable_name: cons.name,
        deliver_subject: cons.deliver_subject,
        deliver_policy: cons.deliver_policy,
        ack_policy: cons.ack_policy,
        ack_wait: cons.ack_wait,
        max_deliver: cons.max_deliver,
        replay_policy: cons.replay_policy
      }
    } |> Jason.encode!()
  end

  defp to_consumer_config(raw) do
    %{
      durable_name: Map.get(raw, "durable_name"),
      deliver_policy: raw |> Map.get("deliver_policy") |> to_sym(),
      deliver_subject: raw |> Map.get("deliver_subject"),
      ack_wait: raw |> Map.get("ack_wait"),
      ack_policy: raw |> Map.get("ack_policy") |> to_sym(),
      replay_policy: raw |> Map.get("replay_policy") |> to_sym(),
      filter_subject: raw |> Map.get("filter_subject"),
      opt_start_time: raw |> Map.get("opt_start_time") |> to_datetime(),
      opt_start_seq: raw |> Map.get("opt_start_seq")
    }
  end

  defp to_consumer_response(raw) do
    %{
      stream_name: Map.get(raw, "stream_name"),
      name: Map.get(raw, "name"),
      created: raw |> Map.get("created") |> to_datetime(),
      config: to_consumer_config(Map.get(raw, "config")),
      delivered: %{
        consumer_seq: get_in(raw, ["delivered", "consumer_seq"]),
        stream_seq: get_in(raw, ["delivered", "stream_seq"])
      },
      ack_floor: %{
        consumer_seq: get_in(raw, ["ack_floor", "consumer_seq"]),
        stream_seq: get_in(raw, ["ack_floor", "stream_seq"])
      },
      num_pending: Map.get(raw, "num_pending"),
      num_redelivered: Map.get(raw, "num_redelivered")
    }
  end
end
