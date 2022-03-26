defmodule Jetstream.PullConsumer.ConnectionOptions do
  @moduledoc false

  @default_retry_timeout 1000
  @default_retries :infinite
  @default_inbox_prefix "_INBOX."

  @enforce_keys [
    :connection_name,
    :stream_name,
    :consumer_name,
    :connection_retry_timeout,
    :connection_retries,
    :inbox_prefix
  ]

  defstruct @enforce_keys

  # Remove this hackery when we will support Elixir ~> 1.13 only.
  if Kernel.function_exported?(Keyword, :validate!, 2) do
    def validate!(connection_options) do
      struct!(
        __MODULE__,
        Keyword.validate!(connection_options, [
          :connection_name,
          :stream_name,
          :consumer_name,
          connection_retry_timeout: @default_retry_timeout,
          connection_retries: @default_retries,
          inbox_prefix: @default_inbox_prefix
        ])
      )
    end
  else
    def validate!(connection_options) do
      struct!(
        __MODULE__,
        Keyword.merge(
          [
            connection_retry_timeout: @default_retry_timeout,
            connection_retries: @default_retries,
            inbox_prefix: @default_inbox_prefix
          ],
          connection_options
        )
      )
    end
  end
end
