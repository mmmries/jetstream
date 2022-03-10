defmodule Jetstream.API.ConsumerDocTest do
  use ExUnit.Case
  doctest Jetstream.API.Consumer

  setup do
    {Gnat, %{}}
    |> Supervisor.child_spec(start: {Gnat, :start_link, [%{}, [name: :gnat]]})
    |> start_supervised!()

    :ok
  end
end
