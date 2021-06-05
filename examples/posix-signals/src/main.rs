/// This example shows how an actor can reply to POSIX signals
/// that the process receives.
///
/// This requires that the `posix-signal-support` feature be
/// enabled, which it is by default.
extern crate pantomime;

use pantomime::prelude::*;
use pantomime::posix_signals::{PosixSignal, PosixSignals};
use std::io;

struct MyActor;

enum MyMsg {
    Sig(PosixSignal)
}

impl Actor for MyActor {
    type Msg = MyMsg;

    fn receive(&mut self, message: Self::Msg, _context: &mut ActorContext<Self::Msg>) {
        match message {
            MyMsg::Sig(value) => {
                println!("received {:?}", value);
            }
        }
    }

    fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<Self::Msg>) {
        match signal {
            Signal::Started => {
                context.watch(PosixSignals, MyMsg::Sig);
            }

            _ => {}
        }
    }
}

fn main() -> io::Result<()> {
    ActorSystem::new().spawn(MyActor)
}
