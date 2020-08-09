defmodule Jetstream.StreamTest do
  use ExUnit.Case
  alias Jetstream.Stream

  test "listing and creating streams" do
    conn = gnat()
    {:ok, %{streams: streams}} = Stream.list(conn)

    assert streams == nil || !("LIST_TEST" in streams)

    {:ok, _} = Stream.create(conn, %{name: "LIST_TEST", subjects: ["LIST_TEST"]})
    {:ok, %{streams: streams}} = Stream.list(conn)

    assert "LIST_TEST" in streams
  end

  defp gnat do
    {:ok, pid} = Gnat.start_link()
    pid
  end
end
