defmodule Jetstream.API.Object.Meta do
  @enforce_keys [:bucket, :chunks, :digest, :name, :nuid, :size]
  defstruct [:bucket, :chunks, :digest, :name, :nuid, :size]
end
