/// This example shows one way you could model an FSM with
/// Pantomime actors.
extern crate pantomime;

use pantomime::prelude::*;
use std::{io, time::Duration};

enum State {
    A,
    B,
}

enum Msg {
    Tick,
    Transition,
}

struct MyActor {
    state: State,
}

impl MyActor {
    fn new() -> Self {
        Self { state: State::A }
    }

    fn receive_a(&mut self, message: Msg, _context: &mut ActorContext<Msg>) {
        match message {
            Msg::Transition => {
                self.state = State::B;
            }

            Msg::Tick => {
                println!("a received tick");
            }
        }
    }

    fn receive_b(&mut self, message: Msg, _context: &mut ActorContext<Msg>) {
        match message {
            Msg::Transition => {
                self.state = State::A;
            }

            Msg::Tick => {
                println!("b received tick");
            }
        }
    }
}

impl Actor for MyActor {
    type Msg = Msg;

    fn receive(&mut self, message: Msg, context: &mut ActorContext<Msg>) {
        match self.state {
            State::A => {
                self.receive_a(message, context);
            }

            State::B => {
                self.receive_b(message, context);
            }
        }
    }

    fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<Msg>) {
        if let Signal::Started = signal {
            context.schedule_periodic_delivery("transition", Duration::from_millis(2000), Duration::from_millis(2000), || {
                Msg::Transition
            });
            context.schedule_periodic_delivery("tick", Duration::from_millis(100), Duration::from_millis(100), || Msg::Tick);
        }
    }
}

fn main() -> io::Result<()> {
    ActorSystem::new().spawn(MyActor::new())
}
