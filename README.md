# Jetstream

A library for interacting with NATS [Jetstream](https://github.com/nats-io/jetstream) from Elixir.
This library builds on to of [gnat](https://hex.pm/packages/gnat), the officially supported Elixir client for NATS.

## Development

The test suite of this project is designed to interact with a live jetstream/NATS server.
You can start one easily with this command: `docker run --rm -ti --name jetstream -p 4222:4222 synadia/jsm:latest server -DV`

You can also use that docker container to access the `nats` cli tool: `docker run -ti --link jetstream synadia/jsm:latest`