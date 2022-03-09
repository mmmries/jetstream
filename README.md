# Jetstream

[![Build Status](https://github.com/mmmries/jetstream/workflows/CI/badge.svg)](https://github.com/mmmries/jetstream/actions?query=workflow%3ACI)
[![Version](https://img.shields.io/hexpm/v/jetstream.svg)](https://hex.pm/packages/jetstream)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/jetstream/)
[![Download](https://img.shields.io/hexpm/dt/jetstream.svg)](https://hex.pm/packages/jetstream)
[![License](https://img.shields.io/hexpm/l/jetstream)][LICENSE]
[![Last Updated](https://img.shields.io/github/last-commit/mmmries/jetstream.svg)](https://github.com/mmmries/jetstream/commits/master)

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
