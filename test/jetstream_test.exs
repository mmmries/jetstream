defmodule JetstreamTest do
  use ExUnit.Case
  doctest Jetstream

  test "greets the world" do
    assert Jetstream.hello() == :world
  end
end
