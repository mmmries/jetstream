ExUnit.start()

# cleanup any streams left over by previous test runs
{:ok, conn} = Gnat.start_link()
{:ok, %{streams: streams}} = Jetstream.API.Stream.list(conn)
streams = streams || []
Enum.each(streams, fn(stream) ->
  :ok = Jetstream.API.Stream.delete(conn, stream)
end)
:ok = Gnat.stop(conn)
