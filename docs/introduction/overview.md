# Overview

[Jetstream](https://docs.nats.io/nats-concepts/jetstream) is a distributed persistence system
built-in to [NATS](https://nats.io/). It provides a streaming system that lets you capture streams
of events from various sources and persist these into persistent stores, which you can immediately
or later replay for processing.

This library exposes interfaces for publishing, consuming and managing Jetstream services. It builds
on top of [Gnat](https://hex.pm/packages/gnat), the officially supported Elixir client for NATS.

> #### Note {: .info}
>
> Jetstream is currently considered a **beta** feature of the NATS project, it is not available
> by default. Any APIs you see here are liable to change in a way that is outside the normal
> versioning scheme.

[Let's get Jetstream up and running](./getting_started.md).
