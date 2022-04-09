defmodule Jetstream.PullConsumer.Job do
  def run(message, module, listening_topic) do
    case module.handle_message(message, nil) do
      {:ack, _state} ->
        Jetstream.ack_next(message, listening_topic)
    end
  end
end
