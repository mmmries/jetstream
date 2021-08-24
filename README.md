# Jetstream

A library for interacting with NATS [Jetstream](https://github.com/nats-io/jetstream) from Elixir.
This library builds on to of [gnat](https://hex.pm/packages/gnat), the officially supported Elixir client for NATS.

## Introduction

There are 3 primary use-cases supported in this library:

1) [Managing Streams and Consumers](MANAGING.md)
2) Publishing messages safely
3) Acting as a consumer

## Development

The test suite of this project is designed to interact with a live jetstream/NATS server.
You can start one easily with this command: `docker-compose up`

Or you can do `docker-compose up --no-start` and then use `docker-compose start` and `docker-compose stop` to start and stop the server.
