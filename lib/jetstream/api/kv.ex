defmodule Jetstream.API.KV do
  @moduledoc """
  API for interacting with the Key/Value store functionality in Nats Jetstream.

  Learn about the Key/Value store: https://docs.nats.io/nats-concepts/jetstream/key-value-store
  """
  alias Jetstream.API.Stream

  @doc """
  Create a new Key/Value bucket

  ## Examples

     iex>{:ok, info} = Jetstream.API.KV.create_bucket(:gnat, "my_bucket")
  """
  @spec create_bucket(conn :: Gnat.t(), bucket_name :: binary()) ::
          {:ok, Stream.info()} | {:error, any()}
  def create_bucket(conn, bucket_name) do
    # The primary NATS docs don't provide information about how to interact
    # with Key-Value functionality over the wire. Turns out the KV store is
    # just a Stream under-the-hood
    # Discovered these settings from looking at the `nats-server -js -DV` logs
    # as well as the GoLang implementation https://github.com/nats-io/nats.go/blob/dd91b86bc4f7fa0f061fefe11506aaee413bfafd/kv.go#L339
    # If the settings aren't correct, NATS will not consider it a valid KV store
    stream = %Stream{
      name: stream_name(bucket_name),
      subjects: stream_subjects(bucket_name),
      max_msgs_per_subject: 1,
      discard: :new,
      allow_rollup_hdrs: true
    }

    Stream.create(conn, stream)
  end

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
  @spec create_key(conn :: Gnat.t(), bucket_name :: binary, key :: binary(), value :: binary()) ::
          :ok
  def create_key(conn, bucket_name, key, value) do
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

  defp stream_name(bucket_name) do
    "KV_#{bucket_name}"
  end

  defp stream_subjects(bucket_name) do
    ["#{subject_prefix(bucket_name)}.>"]
  end

  defp key_name(bucket_name, key) do
    "#{subject_prefix(bucket_name)}.#{key}"
  end

  defp subject_prefix(bucket_name) do
    "$KV.#{bucket_name}"
  end
end
