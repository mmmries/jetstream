defmodule Jetstream.API.KV do
  alias Jetstream.API.Stream

  def create_bucket(conn, bucket_name) do
    stream = %Stream{
      name: "KV_#{bucket_name}",
      subjects: ["$KV.#{bucket_name}.>"],
      max_msgs_per_subject: 1,
      discard: :new,
      allow_rollup_hdrs: true
    }

    Stream.create(conn, stream)
  end
end
