# Getting Started

In this guide, we're going to learn how to install Jetstream in your project and start consuming
messages from your streams.

## Starting Jetstream

The following Docker Compose file will do the job:

```yaml
version: 3
services:
  nats:
    image: nats:latest
    command:
      - -js
    ports:
      - 4222:4222
```

Save this snippet as `docker-compose.yml` and run the following command:

```shell
docker compose up -d
```

Let's also create Jetstream stream where we will publish

```shell
nats stream add hello --subjects="greetings"
```

## Adding Jetstream and Gnat to an application

To start off with, we'll generate a new Elixir application by running this command:

```
mix new hello_jetstream --sup
```

We need to have [a supervision tree](http://elixir-lang.org/getting-started/mix-otp/supervisor-and-application.html)
up and running in your app, and the `--sup` option ensures that.

To add Jetstream to this application, you need to add [Jetstream](https://hex.pm/packages/jetstream)
and [Gnat](https://hex.pm/packages/gnat) libraries to your `deps` definition in our `mix.exs` file:

```elixir
defp deps do
  [
    {:gnat, "~> 1.4"},
    {:jetstream, "~> 0.1"}
  ]
end
```

To install these dependencies, we will run this command:

```shell
mix deps.get
```

Now let's connect to our NATS server. To do this, you need to start `Gnat.ConnectionSupervisor`
under our application's supervision tree. Add following to `lib/hello_jetstream/application.ex`:

```elixir
def start(_type, _args) do
  children = [
    # Create NATS connection
    {Gnat.ConnectionSupervisor, %{
      name: :gnat,
      connection_settings: [
        %{host: "localhost", port: 4222}
      ]
    }},
  ]

  ...
```

This piece of configuration will start Gnat processes that connect to the NATS server and allow
publishing and subscribing to any topics. Jetstream operates using plain NATS topics which follow
specific naming and message format conventions.

Let's now create a _pull consumer_ which will subscribe a specific Jetstream stream and print
incoming messages to standard output.

## Creating a pull consumer

Jetstream offers two stream consuming modes: _push_ and _pull_.

TODO: Pull consumer
TODO: Add nats consumer
TODO: Add to supervision tree

## Publising messages to streams

TODO:

## Running

TODO: Run app
TODO: See output
