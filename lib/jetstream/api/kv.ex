defmodule Jetstream.API.KV do
  @moduledoc """
  API for interacting with the Key/Value store functionality in Nats Jetstream.

  Learn about the Key/Value store: https://docs.nats.io/nats-concepts/jetstream/key-value-store
  """
  alias Jetstream.API.{Consumer, Stream, Util}

  @stream_prefix "KV_"
  @subject_prefix "$KV."
  @two_minutes_in_nanoseconds 1_200_000_000

  @type bucket_options ::
          {:history, non_neg_integer()}
          | {:ttl, non_neg_integer()}
          | {:max_bucket_size, non_neg_integer()}
          | {:max_value_size, non_neg_integer()}
          | {:description, binary()}
          | {:replicas, non_neg_integer()}
          | {:storage, :file | :memory}
          | {:placement, Stream.placement()}

  @doc """
  Create a new Key/Value bucket. Can include the following options

  * `:history` - How many historic values to keep per key (defaults to 1, max of 64)
  * `:ttl` - How long to keep values for (in nanoseconds)
  * `:max_bucket_size` - The max number of bytes the bucket can hold
  * `:max_value_size` - The max number of bytes a value may be
  * `:description` - A description for the bucket
  * `:replicas` - How many replicas of the data to store
  * `:storage` - Storage backend to use (:file, :memory)
  * `:placement` - A map with :cluster (required) and :tags (optional)

  ## Examples

     iex>{:ok, info} = Jetstream.API.KV.create_bucket(:gnat, "my_bucket")
  """
  @spec create_bucket(conn :: Gnat.t(), bucket_name :: binary(), params :: [bucket_options()]) ::
          {:ok, Stream.info()} | {:error, any()}
  def create_bucket(conn, bucket_name, params \\ []) do
    # The primary NATS docs don't provide information about how to interact
    # with Key-Value functionality over the wire. Turns out the KV store is
    # just a Stream under-the-hood
    # Discovered these settings from looking at the `nats-server -js -DV` logs
    # as well as the GoLang implementation https://github.com/nats-io/nats.go/blob/dd91b86bc4f7fa0f061fefe11506aaee413bfafd/kv.go#L339
    # If the settings aren't correct, NATS will not consider it a valid KV store
    stream = %Stream{
      name: stream_name(bucket_name),
      subjects: stream_subjects(bucket_name),
      description: Keyword.get(params, :description),
      max_msgs_per_subject: Keyword.get(params, :history, 1),
      discard: :new,
      deny_delete: true,
      allow_rollup_hdrs: true,
      max_age: Keyword.get(params, :ttl, 0),
      max_bytes: Keyword.get(params, :max_bucket_size, -1),
      max_msg_size: Keyword.get(params, :max_value_size, -1),
      num_replicas: Keyword.get(params, :replicas, 1),
      storage: Keyword.get(params, :storage, :file),
      placement: Keyword.get(params, :placement),
      duplicate_window: adjust_duplicate_window(Keyword.get(params, :ttl, 0))
    }

    Stream.create(conn, stream)
  end

  # The `duplicate_window` can't be greater than the `max_age`. The default `duplicate_window`
  # is 2 minutes. We'll keep the 2 minute window UNLESS the ttl is less than 2 minutes
  defp adjust_duplicate_window(ttl) when ttl > 0 and ttl < @two_minutes_in_nanoseconds, do: ttl
  defp adjust_duplicate_window(_ttl), do: @two_minutes_in_nanoseconds

  @doc """
  Delete a Key/Value bucket

  ## Examples

     iex>:ok = Jetstream.API.KV.delete_bucket(:gnat, "my_bucket")
  """
  @spec delete_bucket(conn :: Gnat.t(), bucket_name :: binary()) :: :ok | {:error, any()}
  def delete_bucket(conn, bucket_name) do
    Stream.delete(conn, stream_name(bucket_name))
  end

  @doc """
  Create a Key in a Key/Value Bucket

  ## Examples

      iex>:ok = Jetstream.API.KV.create_key(:gnat, "my_bucket", "my_key", "my_value")
  """
  @spec create_key(conn :: Gnat.t(), bucket_name :: binary(), key :: binary(), value :: binary()) ::
          :ok
  def create_key(conn, bucket_name, key, value) do
    Gnat.pub(conn, key_name(bucket_name, key), value)
  end

  @doc """
  Delete a Key from a K/V Bucket

  ## Examples

      iex>:ok = Jetstream.API.KV.delete_key(:gnat, "my_bucket", "my_key")
  """
  @spec delete_key(conn :: Gnat.t(), bucket_name :: binary(), key :: binary()) :: :ok
  def delete_key(conn, bucket_name, key) do
    Gnat.pub(conn, key_name(bucket_name, key), "", headers: [{"KV-Operation", "DEL"}])
  end

  @doc """
  Purge a Key from a K/V bucket. This will remove any revision history the key had

  ## Examples

      iex>:ok = Jetstream.API.KV.purge_key(:gnat, "my_bucket", "my_key")
  """
  @spec purge_key(conn :: Gnat.t(), bucket_name :: binary(), key :: binary()) :: :ok
  def purge_key(conn, bucket_name, key) do
    Gnat.pub(conn, key_name(bucket_name, key), "",
      headers: [{"KV-Operation", "PURGE"}, {"Nats-Rollup", "sub"}]
    )
  end

  @doc """
  Put a value into a Key in a K/V Bucket

  ## Examples

      iex>:ok = Jetstream.API.KV.put_value(:gnat, "my_bucket", "my_key", "my_value")
  """
  @spec put_value(conn :: Gnat.t(), bucket_name :: binary(), key :: binary(), value :: binary()) ::
          :ok
  def put_value(conn, bucket_name, key, value) do
    Gnat.pub(conn, key_name(bucket_name, key), value)
  end

  @doc """
  Get the value for a key in a particular K/V bucket

  ## Examples

      iex>"my_value" = Jetstream.API.KV.get_value(:gnat, "my_bucket", "my_key")
  """
  @spec get_value(conn :: Gnat.t(), bucket_name :: binary(), key :: binary()) ::
          binary() | {:error, any()}
  def get_value(conn, bucket_name, key) do
    case Stream.get_message(conn, stream_name(bucket_name), %{
           last_by_subj: key_name(bucket_name, key)
         }) do
      {:ok, message} -> message.data
      error -> error
    end
  end

  @doc """
  Get all the non-deleted keys for a Key Value Bucket

  ## Examples

      iex>["key1", "key2"] = Jetstream.API.KV.list_keys(:gnat, "my_bucket")
  """
  @spec list_keys(conn :: Gnat.t(), bucket_name :: binary()) :: [binary()]
  def list_keys(conn, bucket_name) do
    stream = stream_name(bucket_name)
    inbox = Util.reply_inbox()
    consumer_name = "list_keys_consumer_#{Util.nuid()}"

    {:ok, sub} = Gnat.sub(conn, self(), inbox)

    {:ok, _consumer} =
      Consumer.create(conn, %Consumer{
        durable_name: consumer_name,
        deliver_subject: inbox,
        stream_name: stream,
        ack_policy: :none,
        max_ack_pending: -1,
        max_deliver: 1
      })

    keys =
      receive_keys()
      |> Enum.map(fn key -> String.replace(key, "#{@subject_prefix}#{bucket_name}.", "") end)
      |> Enum.sort()

    :ok = Gnat.unsub(conn, sub)
    :ok = Consumer.delete(conn, stream, consumer_name)

    keys
  end

  defp receive_keys() do
    gather_messages = fn
      func, keys ->
        receive do
          {:msg, %{topic: key, headers: headers}} ->
            if {"kv-operation", "DEL"} in headers do
              func.(func, keys)
            else
              func.(func, [key | keys])
            end

          {:msg, %{topic: key}} ->
            func.(func, [key | keys])
        after
          100 ->
            keys
        end
    end

    gather_messages.(gather_messages, [])
  end

  defp stream_name(bucket_name) do
    "#{@stream_prefix}#{bucket_name}"
  end

  defp stream_subjects(bucket_name) do
    ["#{@subject_prefix}#{bucket_name}.>"]
  end

  defp key_name(bucket_name, key) do
    "#{@subject_prefix}#{bucket_name}.#{key}"
  end
end
