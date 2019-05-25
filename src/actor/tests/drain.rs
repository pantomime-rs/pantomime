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
            } else {
            }
        }
    }

    fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<usize>) {
        match signal {
            Signal::Started => {
                if self.id == 0 {
                    for id in 1..=LIMIT {
                        let a = context.spawn(MyActor {
                            id,
                            actor_ref: context.actor_ref().clone(),
                            count: 0,
                        });

                        for _ in 0..TIMES {
                            a.tell(1);
                        }

                        a.stop();
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
    struct TestReaper;

    impl Actor<()> for TestReaper {
        fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
            if let Signal::Started = signal {
                let mut probe = ctx.spawn_probe::<usize>();

                ctx.spawn(MyActor {
                    id: 0,
                    actor_ref: probe.actor_ref().clone(),
                    count: 0,
                });

                assert_eq!(probe.receive(Duration::from_secs(10)), LIMIT * TIMES);

                ctx.actor_ref().stop();
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
