//! # Pantomime
//!
//! Pantomime is an [Actor model](https://en.wikipedia.org/wiki/Actor_model) library
//! for [Rust](https://www.rust-lang.org/). The Actor model defines a powerful computation
//! abstraction that elevates the realities of distributed systems to empower you to build
//! highly concurrent and performant systems.

extern crate atom;
extern crate atty;
extern crate chrono;
extern crate crossbeam;
extern crate fern;
extern crate parking_lot;
extern crate rand;

#[macro_use]
extern crate downcast_rs;

#[macro_use]
extern crate log;

#[cfg(feature = "futures-support")]
extern crate futures;

#[cfg(feature = "tokio-support")]
extern crate tokio as ext_tokio;

#[cfg(feature = "tokio-support")]
extern crate tokio_executor;

#[cfg(feature = "tokio-support")]
extern crate tokio_reactor;

#[cfg(feature = "tokio-support")]
extern crate tokio_threadpool;

#[cfg(feature = "tokio-support")]
extern crate tokio_timer;

pub mod actor;
pub mod cfg;
pub mod dispatcher;
pub mod mailbox;
pub mod pattern;
pub mod prelude;
pub mod timer;
pub mod util;

#[cfg(feature = "testkit")]
pub mod testkit;

#[cfg(test)]
pub mod testkit;
