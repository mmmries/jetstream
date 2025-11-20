# DEPRECATED

This library is no longer maintained, and all functionality has been moved back into the main `gnat` library (see nats-io/nats.ex#146).
This repo will be left here for reference only.
We initially opened it as a separate repo so we could experiment with the API that we wanted to expose, but eventually decided that it was easier for users to have a single NATS client library that handled both jetstream and non-jetstream aspects of interacting with NATS.

# Jetstream

A library for interacting with NATS [Jetstream](https://github.com/nats-io/jetstream) from Elixir.
This library builds on top of [gnat](https://hex.pm/packages/gnat), the officially supported Elixir
client for NATS.

## Development

The test suite of this project is designed to interact with a live NATS/Jetstream server.
You can start tests easily with these commands:

```shell
docker compose up -d
mix test
```

## License

See the [LICENSE] and [NOTICE] files for license rights and limitations.

[LICENSE]: https://github.com/mmmries/jetstream/blob/master/LICENSE.txt
[NOTICE]: https://github.com/mmmries/jetstream/blob/master/NOTICE
