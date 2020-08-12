defmodule Jetstream.Consumer do
  @enforce_keys [:stream_name, :name]
  defstruct stream_name: nil,
            name: nil,
            deliver_subject: nil,
            deliver_policy: :all,
            durable: true,
            ack_policy: :explicit,
            ack_await: 30_000_000_000, # 30sec
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
    deliver_policy: :all | :last | :new,
    deliver_subject: nil | binary(),
    ack_await: nil | non_neg_integer(),
    ack_policy: :none | :all | :explicit,
    replay_policy: :instant | :original,
    filter_subject: nil | binary(),
    opt_start_time: nil | DateTime.t(),
    opt_start_seq: nil | non_neg_integer()
  }

  @type t :: %__MODULE__{
    stream_name: binary(),
    name: binary(),
  }


end
