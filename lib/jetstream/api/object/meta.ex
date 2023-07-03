defmodule Jetstream.API.Object.Meta do
  @derive Jason.Encoder
  @enforce_keys [:bucket, :chunks, :digest, :name, :nuid, :size]
  defstruct [:bucket, :chunks, :digest, :name, :nuid, :size]
end
