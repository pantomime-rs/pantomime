use crate::actor::*;
use std::time::Duration;

const LIMIT: usize = 1024;
const TIMES: usize = 128;

struct MyActor {
    id: usize,
    actor_ref: ActorRef<usize>,
    count: usize,
}

impl Actor<usize> for MyActor {
    fn receive(&mut self, msg: usize, _context: &mut ActorContext<usize>) {
        if self.id != 0 {
            self.actor_ref.tell(msg);
        } else {
            self.count += msg;

            if self.count == LIMIT * TIMES {
                self.actor_ref.tell(self.count);
            }
        }
    }

    fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<usize>) {
        match signal {
            Signal::Started => {
                if self.id == 0 {
                    for id in 1..LIMIT + 1 {
                        let a = context.spawn(MyActor {
                            id,
                            actor_ref: context.actor_ref().clone(),
                            count: 0,
                        });

                        for _ in 0..TIMES {
                            a.tell(1);
                        }

                        a.drain();
                    }
                }
            }

            Signal::Stopped => {
                if self.id == 0 {
                    self.actor_ref.tell(self.count);
                }
            }

            _ => (),
        }
    }
}

#[test]
fn test() {
    let mut system = ActorSystem::new().start();

    let mut probe = system.spawn_probe::<usize>();

    system.spawn(MyActor {
        id: 0,
        actor_ref: probe.actor_ref.clone(),
        count: 0,
    });

    assert_eq!(probe.receive(Duration::from_secs(10)), LIMIT * TIMES);

    system.context.drain();

    system.join();
}
