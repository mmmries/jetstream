defmodule Jetstream.API.Object do
  @moduledoc """
  API for interacting with the JetStream Object Store

  Learn more about Object Store: https://docs.nats.io/nats-concepts/jetstream/obj_store
  """
  alias Jetstream.API.{Stream}

  @stream_prefix "OBJ_"
  @subject_prefix "$O."

  def create_bucket(conn, bucket_name, params \\ []) do
    with :ok <- validate_bucket_name(bucket_name) do
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
  end

  def delete_bucket(conn, bucket_name) do
    Stream.delete(conn, stream_name(bucket_name))
  end

  defp stream_name(bucket_name) do
    "#{@stream_prefix}#{bucket_name}"
  end

  defp stream_subjects(bucket_name) do
    [
      chunk_stream_subject(bucket_name),
      meta_stream_subject(bucket_name)
    ]
  end

  defp chunk_stream_subject(bucket_name) do
    "#{@subject_prefix}#{bucket_name}.C.>"
  end

  defp meta_stream_subject(bucket_name) do
    "#{@subject_prefix}#{bucket_name}.M.>"
  end

  @two_minutes_in_nanoseconds 1_200_000_000
  # The `duplicate_window` can't be greater than the `max_age`. The default `duplicate_window`
  # is 2 minutes. We'll keep the 2 minute window UNLESS the ttl is less than 2 minutes
  defp adjust_duplicate_window(ttl) when ttl > 0 and ttl < @two_minutes_in_nanoseconds, do: ttl
  defp adjust_duplicate_window(_ttl), do: @two_minutes_in_nanoseconds

  defp validate_bucket_name(name) do
    case Regex.match?(~r/^[a-zA-Z0-9_-]+$/, name) do
      true -> :ok
      false -> {:error, "invalid bucket name"}
    end
  end
end
