defmodule Jetstream.API.ConsumerDocTest do
  use Jetstream.ConnCase
  @moduletag with_gnat: :gnat
  doctest Jetstream.API.Consumer
end
