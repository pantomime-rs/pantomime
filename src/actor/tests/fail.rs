use crate::actor::*;
use std::time::Duration;

struct MyActor {
    id: usize,
    actor_ref: ActorRef<()>,
}

impl Actor<()> for MyActor {
    fn receive(&mut self, _: (), _context: &mut ActorContext<()>) {
        if self.id == 1 {
            panic!();
        }
    }

    fn receive_signal(&mut self, signal: Signal, _: &mut ActorContext<()>) {
        match signal {
            Signal::Started => {
                if self.id == 0 {
                    panic!();
                }
            }

            Signal::Failed => {
                self.actor_ref.tell(());
            }

            _ => (),
        }
    }
}

impl Drop for MyActor {
    fn drop(&mut self) {
        if self.id == 2 {
            self.actor_ref.tell(());
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
                let mut probe = ctx.spawn_probe::<()>();

                // 0 panics during startup
                ctx.spawn(MyActor {
                    id: 0,
                    actor_ref: probe.actor_ref().clone(),
                });

                assert_eq!(probe.receive(Duration::from_secs(10)), ());

                // 1 panics after receiving a message
                let one = ctx.spawn(MyActor {
                    id: 1,
                    actor_ref: probe.actor_ref().clone(),
                });

                one.tell(());

                assert_eq!(probe.receive(Duration::from_secs(10)), ());

                // 2 messages in its drop method

                let two = ctx.spawn(MyActor {
                    id: 2,
                    actor_ref: probe.actor_ref().clone(),
                });

                two.stop();

                assert_eq!(probe.receive(Duration::from_secs(10)), ());

                ctx.actor_ref().stop();
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
