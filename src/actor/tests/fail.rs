use crate::actor::*;
use std::io;
use std::time::Duration;

struct MyActor {
    id: usize,
    actor_ref: ActorRef<usize>,
}

impl Actor<()> for MyActor {
    fn receive(&mut self, _: (), context: &mut ActorContext<()>) {
        if self.id == 1 {
            panic!();
        } else if self.id == 4 {
            self.id = 40;

            // the reaper sent a few messages, so what we're testing
            // here is that context.fail is received but those
            // other messages are not

            context.fail(io::Error::new(io::ErrorKind::Other, "error"));
        } else if self.id == 40 {
            self.actor_ref.tell(1);
        }
    }

    fn receive_signal(&mut self, signal: Signal, _: &mut ActorContext<()>) {
        match signal {
            Signal::Started => {
                if self.id == 0 {
                    panic!();
                }
            }

            Signal::Stopped(Some(_)) => {
                self.actor_ref.tell(0);
            }

            _ => (),
        }
    }
}

impl Drop for MyActor {
    fn drop(&mut self) {
        if self.id == 2 {
            self.actor_ref.tell(0);
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

                // 0 panics during startup
                ctx.spawn(MyActor {
                    id: 0,
                    actor_ref: probe.actor_ref().clone(),
                });

                assert_eq!(probe.receive(Duration::from_secs(10)), 0);

                // 1 panics after receiving a message
                let one = ctx.spawn(MyActor {
                    id: 1,
                    actor_ref: probe.actor_ref().clone(),
                });

                one.tell(());

                assert_eq!(probe.receive(Duration::from_secs(10)), 0);

                // 2 messages in its drop method

                let two = ctx.spawn(MyActor {
                    id: 2,
                    actor_ref: probe.actor_ref().clone(),
                });

                two.stop();

                assert_eq!(probe.receive(Duration::from_secs(10)), 0);

                // 3 is failed from the outside

                let three = ctx.spawn(MyActor {
                    id: 3,
                    actor_ref: probe.actor_ref().clone(),
                });

                three.fail(FailureError::new(io::Error::new(
                    io::ErrorKind::Other,
                    "failed",
                )));

                assert_eq!(probe.receive(Duration::from_secs(10)), 0);

                // 4 fails from inside (testing message order)

                let four = ctx.spawn(MyActor {
                    id: 4,
                    actor_ref: probe.actor_ref().clone(),
                });

                four.tell(());
                four.tell(()); // shouldn't be received
                four.tell(()); // shouldn't be received
                four.tell(()); // shouldn't be received

                assert_eq!(probe.receive(Duration::from_secs(10)), 0);

                ctx.actor_ref().stop();
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
