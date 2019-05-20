/// In this example, we have a function that returns a future
/// that is completed by the scheduler in 1 second.
///
/// While this is a trivial example, it's easy to see how
/// `Future`s integrate nicely with Actors.
///
/// For those coming from the Akka/Scala ecosystem, a oneshot
/// is akin to a `scala.concurrent.Promise`.
///
/// At the core of the ActorSystem is a dispatcher that implements
/// a work-stealing scheduler that runs futures and does messaging.
extern crate futures;
extern crate pantomime;

use futures::*;
use pantomime::pattern::{Ask, PipeTo};
use pantomime::prelude::*;
use std::{io, process, time};

const DELAY_MS: u64 = 1000;

fn slow_double(
    context: &ActorContext<Msg>,
    value: usize,
) -> impl Future<Item = usize, Error = Canceled> {
    let (c, p) = oneshot::<usize>();

    context.schedule_thunk(time::Duration::from_millis(DELAY_MS), move || {
        let _ = c.send(value * 2);
    });

    p
}

enum Msg {
    Double(usize),
    SendDouble(usize, ActorRef<usize>),
    ReceivedDouble(usize),
}

struct MyActor;

impl Actor<Msg> for MyActor {
    fn receive(&mut self, message: Msg, context: &mut ActorContext<Msg>) {
        match message {
            Msg::Double(value) => {
                println!("result: {}", value);

                if value > 4096 {
                    process::exit(0);
                }

                context.spawn_future(
                    slow_double(&context, value)
                        .then(|r| future::ok(Msg::Double(r.ok().unwrap_or(0))))
                        .pipe_to(context.actor_ref().clone()),
                );
            }

            Msg::SendDouble(value, reply_to) => {
                println!("send a double!");
                reply_to.tell(value * 2);
            }

            Msg::ReceivedDouble(value) => {
                println!("received a doubling of {}", value);
            }
        }
    }

    fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<Msg>) {
        if let Signal::Started = signal {
            context.spawn_future(
                context
                    .actor_ref()
                    .ask(time::Duration::from_secs(10), |reply_to| {
                        Msg::SendDouble(42, reply_to)
                    })
                    .then(|r| future::ok(Msg::ReceivedDouble(r.ok().unwrap_or(0))))
                    .pipe_to(context.actor_ref().clone()),
            );
        }
    }
}

struct MyReaper;

impl Actor<()> for MyReaper {
    fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

    fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
        if let Signal::Started = signal {
            let my_actor = ctx.spawn(MyActor);

            my_actor.tell(Msg::Double(2));
        }
    }
}

fn main() -> io::Result<()> {
    ActorSystem::new().spawn(MyReaper)
}
