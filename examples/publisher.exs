{:ok, gnat} = Gnat.start_link()
Gnat.pub(gnat, "greetings.hello", "hello")

Enum.each(1..100, fn(i) ->
  Gnat.pub(gnat, "greetings.hola", "hola #{i}")
end)
