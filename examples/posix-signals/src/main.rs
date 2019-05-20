/// This example shows how an actor can reply to POSIX signals
/// that the process receives.
///
/// This requires that the `posix-signal-support` feature be
/// enabled, which it is by default.
extern crate pantomime;

use pantomime::prelude::*;
use std::io;

struct MyActor;

impl Actor<()> for MyActor {
    fn receive(&mut self, _message: (), _context: &mut ActorContext<()>) {}

    fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<()>) {
        match signal {
            Signal::Started => {
                context.watch_posix_signals();
            }

            Signal::PosixSignal(value) => {
                println!("received {}", value);
            }

            _ => {}
        }
    }
}

fn main() -> io::Result<()> {
    ActorSystem::new().spawn(MyActor)
}
