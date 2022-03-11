defmodule JetstreamTest do
  use Jetstream.ConnCase
  @moduletag with_gnat: :gnat
  doctest Jetstream
end
