# Jetstream

[![Build Status](https://github.com/mmmries/jetstream/workflows/CI/badge.svg)](https://github.com/mmmries/jetstream/actions?query=workflow%3ACI)
[![Version](https://img.shields.io/hexpm/v/jetstream.svg)](https://hex.pm/packages/jetstream)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/jetstream/)
[![Download](https://img.shields.io/hexpm/dt/jetstream.svg)](https://hex.pm/packages/jetstream)
[![License](https://img.shields.io/hexpm/l/jetstream)][LICENSE]
[![Last Updated](https://img.shields.io/github/last-commit/mmmries/jetstream.svg)](https://github.com/mmmries/jetstream/commits/master)

A library for interacting with NATS [Jetstream](https://github.com/nats-io/jetstream) from Elixir.
This library builds on to of [gnat](https://hex.pm/packages/gnat), the officially supported Elixir client for NATS.

## Installation

The package can be installed by adding `jetstream` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:jetstream, "~> 0.0.1-pre2"}]
end
```

## Introduction

There are 3 primary use-cases supported in this library:

1) [Managing Streams and Consumers](MANAGING.md)
2) Publishing messages safely
3) Acting as a consumer

## Development

The test suite of this project is designed to interact with a live jetstream/NATS server.
You can start one easily with this command: `docker-compose up`

Or you can do `docker-compose up --no-start` and then use `docker-compose start` and `docker-compose stop` to start and stop the server.

## License

See the [LICENSE] file for license rights and limitations.

[LICENSE]: https://github.com/mmmries/jetstream/blob/master/LICENSE.txt
