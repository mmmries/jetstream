defmodule Jetstream.API.Object.Meta do
  @derive Jason.Encoder
  @enforce_keys [:bucket, :chunks, :digest, :name, :nuid, :size]
  defstruct bucket: nil,
            chunks: nil,
            deleted: false,
            digest: nil,
            name: nil,
            nuid: nil,
            size: nil
end

defimpl Jason.Encoder, for: Jetstream.API.Object.Meta do
  alias Jetstream.API.Object.Meta

  def encode(%Meta{deleted: true} = meta, opts) do
    Map.take(meta, [:bucket, :chunks, :deleted, :digest, :name, :nuid, :size])
    |> Jason.Encode.map(opts)
  end

  def encode(meta, opts) do
    Map.take(meta, [:bucket, :chunks, :digest, :name, :nuid, :size])
    |> Jason.Encode.map(opts)
  end
end
