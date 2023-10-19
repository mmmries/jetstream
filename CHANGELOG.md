# Changelog

## 0.0.9

* Added support for Jetstream domains https://github.com/mmmries/jetstream/pull/86

## 0.0.8

* Added the `Jetstream.API.Object` module for interacting with the ObjectStore pattern https://github.com/mmmries/jetstream/pull/78

## 0.0.7

* Makes `KV.put_value`, `KV.create_key`, `KV.delete_key`, and `KV.purge_key` synchronous

## 0.0.6

* Adds KV watcher https://github.com/mmmries/jetstream/pull/72

## 0.0.5

* Include NATS headers in `Broadway.Message` metadata https://github.com/mmmries/jetstream/pull/68

## 0.0.4

* fix bug with `Jetstream.API.KV.contents/2` where consumer names sometimes had `/` characters. https://github.com/mmmries/jetstream/pull/66

## 0.0.3

* add `Jetstream.API.KV.contents/2` function https://github.com/mmmries/jetstream/pull/64
