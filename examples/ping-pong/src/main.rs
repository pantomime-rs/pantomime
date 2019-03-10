extern crate pantomime;

use pantomime::prelude::*;
use std::process;

const ACTOR_PAIRS: usize = 8;
const MESSAGE_LIMIT: u64 = 10_000_000;

struct Finisher {
    count: usize,
}

impl Finisher {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl Actor<()> for Finisher {
    fn receive(&mut self, _message: (), _context: &mut ActorContext<()>) {
        self.count += 1;

        info!("{} actors have finished", self.count);

        if self.count == ACTOR_PAIRS {
            process::exit(0);
        }
    }
}

struct Pinger {
    finisher: ActorRef<()>,
}

impl Pinger {
    fn new(finisher: &ActorRef<()>) -> Self {
        Self {
            finisher: finisher.clone(),
        }
    }
}

enum PingerMessage {
    Message {
        count: u64,
        reply_to: ActorRef<PingerMessage>,
    },
    TalkTo {
        to: ActorRef<PingerMessage>,
    },
}

impl Actor<PingerMessage> for Pinger {
    fn receive(&mut self, message: PingerMessage, context: &mut ActorContext<PingerMessage>) {
        match message {
            PingerMessage::Message { count, reply_to } => {
                if count == MESSAGE_LIMIT {
                    self.finisher.tell(());

                    context.actor_ref().stop();
                } else {
                    reply_to.tell(PingerMessage::Message {
                        count: count + 1,
                        reply_to: context.actor_ref().clone(),
                    });
                }
            }

            PingerMessage::TalkTo { to } => {
                info!("started: {}", context.actor_ref().id());
                to.tell(PingerMessage::Message {
                    count: 0,
                    reply_to: context.actor_ref().clone(),
                });
            }
        }
    }
}

fn main() {
    let mut system = ActorSystem::new().start();

    let finisher = system.spawn(Finisher::new());

    for _ in 0..ACTOR_PAIRS {
        let pinger_a = {
            let finisher = finisher.clone();

            system.spawn(Pinger::new(&finisher))
        };

        let pinger_b = {
            let finisher = finisher.clone();

            system.spawn(Pinger::new(&finisher))
        };

        pinger_a.tell(PingerMessage::TalkTo { to: pinger_b });
    }

    system.join();
}
