defmodule Jetstream.Util do
  @moduledoc false

  @doc "Generate a NATS Unique identifier"
  def nuid() do
    :crypto.strong_rand_bytes(12) |> Base.encode64()
  end
end
