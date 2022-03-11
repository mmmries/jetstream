defmodule Jetstream.ConnCase do
  @moduledoc """
  This module defines the test case to be used by tests that require setting up a connection.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import Jetstream.ConnCase
    end
  end

  setup tags do
    if arg = Map.get(tags, :with_gnat) do
      conn = start_gnat!(arg)
      %{conn: conn}
    else
      :ok
    end
  end

  def start_gnat!(name) when is_atom(name) do
    start_gnat!(%{}, name: name)
  end

  def start_gnat!(connection_settings, options)
      when is_map(connection_settings) and is_list(options) do
    {Gnat, %{}}
    |> Supervisor.child_spec(start: {Gnat, :start_link, [connection_settings, options]})
    |> start_supervised!()
  end
end
