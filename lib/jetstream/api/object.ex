defmodule Jetstream.API.Object do
  @moduledoc """
  API for interacting with the JetStream Object Store

  Learn more about Object Store: https://docs.nats.io/nats-concepts/jetstream/obj_store
  """
  alias Jetstream.API.{Stream, Util}

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

  @spec put_object(Gnat.t(), String.t(), String.t(), File.io_device()) ::
          {:ok, map()} | {:error, any()}
  def put_object(conn, bucket_name, object_name, io) do
    nuid = Util.nuid()
    chunk_topic = chunk_stream_topic(bucket_name, nuid)

    with {:ok, %{config: _stream}} <- Stream.info(conn, stream_name(bucket_name)),
         {:ok, chunks, size, digest} <- send_chunks(conn, io, chunk_topic) do
      object_meta = %{
        name: object_name,
        bucket: bucket_name,
        nuid: nuid,
        size: size,
        chunks: chunks,
        digest: "SHA-256=#{Base.encode64(digest)}"
      }

      topic = meta_stream_topic(bucket_name, object_name)
      body = Jason.encode!(object_meta)

      case Gnat.request(conn, topic, body, headers: [{"Nats-Rollup", "sub"}]) do
        {:ok, _} ->
          {:ok, object_meta}

        error ->
          error
      end
    end
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

  defp chunk_stream_topic(bucket_name, nuid) do
    "#{@subject_prefix}#{bucket_name}.C.#{nuid}"
  end

  defp meta_stream_subject(bucket_name) do
    "#{@subject_prefix}#{bucket_name}.M.>"
  end

  defp meta_stream_topic(bucket_name, object_name) do
    key = Base.encode64(object_name)
    "#{@subject_prefix}#{bucket_name}.M.#{key}"
  end

  @two_minutes_in_nanoseconds 1_200_000_000
  # The `duplicate_window` can't be greater than the `max_age`. The default `duplicate_window`
  # is 2 minutes. We'll keep the 2 minute window UNLESS the ttl is less than 2 minutes
  defp adjust_duplicate_window(ttl) when ttl > 0 and ttl < @two_minutes_in_nanoseconds, do: ttl
  defp adjust_duplicate_window(_ttl), do: @two_minutes_in_nanoseconds

  @chunk_size 128 * 1024
  defp send_chunks(conn, io, topic) do
    sha = :crypto.hash_init(:sha256)
    size = 0
    chunks = 0
    send_chunks(conn, io, topic, sha, size, chunks)
  end

  defp send_chunks(conn, io, topic, sha, size, chunks) do
    case IO.binread(io, @chunk_size) do
      :eof ->
        sha = :crypto.hash_final(sha)
        {:ok, chunks, size, sha}

      {:error, err} ->
        {:error, err}

      bytes ->
        sha = :crypto.hash_update(sha, bytes)
        size = size + byte_size(bytes)
        chunks = chunks + 1

        case Gnat.request(conn, topic, bytes) do
          {:ok, _} ->
            send_chunks(conn, io, topic, sha, size, chunks)

          error ->
            error
        end
      end
    end

    defp validate_bucket_name(name) do
      case Regex.match?(~r/^[a-zA-Z0-9_-]+$/, name) do
        true -> :ok
        false -> {:error, "invalid bucket name"}
      end
    end
  end
end
