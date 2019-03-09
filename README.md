# Pantomime

## Overview

Pantomime is an [Actor model](https://en.wikipedia.org/wiki/Actor_model) library
for [Rust](https://www.rust-lang.org/). The Actor model defines a useful computation
abstraction that elevates the realities of distributed systems to empower you to build
highly concurrent and performant systems.

## Examples

We've put together a number of [example programs](examples/) using Pantomime. These include
simple ping/pong style scenarios, futures-rs integration, Hyper integration, and FSM modeling.

## Prior Art

Pantomime isn't the only actor implementation for Rust, but there were several
key motivating factors in creating the project.

The primary goal of the project is make concurrent programming of systems
in Rust as easy and as natural as it is with Akka, but to do so with
better safety and resource utilization that comes with Rust's ownership system.

To help achieve this goal, Pantomime heavily leverages [Crossbeam](https://github.com/crossbeam-rs/crossbeam)
for its concurrent programming primitives.

A custom [Tokio](https://tokio.rs/) executor is provided to integrate Tokio
applications with Pantomime. For instance, [Hyper](https://github.com/hyperium/hyper)
can be used to provide an HTTP interface for your applications that use
Pantomime actors.

## What's next?

This project tackles the core actor model implementation, on which higher level
abstractions can be built. Next up, we'd love to explore a streaming implementation
inspired by the [Reactive Streams](http://www.reactive-streams.org/) initiative.

There's also the usual suspects like event sourcing, clustering, etc.
It would be great to take the project in that direction as well.

## Author

Jason Longshore <hello@jasonlongshore.com>
