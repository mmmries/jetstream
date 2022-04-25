ExUnit.start()

# set assert_receive default timeout
Application.put_env(:ex_unit, :assert_receive_timeout, 1_000)

# cleanup any streams left over by previous test runs
{:ok, conn} = Gnat.start_link()
{:ok, %{streams: streams}} = Jetstream.API.Stream.list(conn)
streams = streams || []

Enum.each(streams, fn stream ->
  :ok = Jetstream.API.Stream.delete(conn, stream)
end)

:ok = Gnat.stop(conn)
