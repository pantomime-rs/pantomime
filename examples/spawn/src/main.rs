extern crate pantomime;

use pantomime::prelude::*;
use std::io;

const NUM_WORKERS: usize = 1_000_000;

struct Worker {
    _id: usize,
}

enum WorkerMsg {
    Ready(ActorRef<()>),
}

impl Actor for Worker {
    type Msg = WorkerMsg;

    fn receive(&mut self, msg: WorkerMsg, _: &mut ActorContext<WorkerMsg>) {
        match msg {
            WorkerMsg::Ready(reply_to) => {
                reply_to.tell(());
            }
        }
    }
}

struct Reaper {
    ready: usize,
}

impl Reaper {
    fn new() -> Self {
        Self { ready: 0 }
    }
}

impl Actor for Reaper {
    type Msg = ();

    fn receive(&mut self, _: (), _: &mut ActorContext<()>) {
        self.ready += 1;

        if self.ready == NUM_WORKERS {
            println!("all workers ready");
        }
    }

    fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
        if let Signal::Started = signal {
            for id in 1..=NUM_WORKERS {
                let worker = ctx.spawn(Worker { _id: id });

                worker.tell(WorkerMsg::Ready(ctx.actor_ref().clone()));
            }
        }
    }
}

fn main() -> io::Result<()> {
    ActorSystem::new().spawn(Reaper::new())
}
