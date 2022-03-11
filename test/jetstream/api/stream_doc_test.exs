defmodule Jetstream.API.StreamDocTest do
  use ExUnit.Case
  doctest Jetstream.API.Stream

  setup do
    {Gnat, %{}}
    |> Supervisor.child_spec(start: {Gnat, :start_link, [%{}, [name: :gnat]]})
    |> start_supervised!()

    :ok
  end
end
