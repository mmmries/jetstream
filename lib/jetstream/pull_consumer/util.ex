defmodule Jetstream.PullConsumer.Util do
  @moduledoc false

  alias Jetstream.PullConsumer.ConnectionOptions

  def new_listening_topic(%ConnectionOptions{} = o) do
    o.inbox_prefix <> nuid()
  end

  defp nuid() do
    :crypto.strong_rand_bytes(12) |> Base.encode64()
  end
end
