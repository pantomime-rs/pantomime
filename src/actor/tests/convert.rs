use crate::actor::*;
use std::time::Duration;

struct MyActor;

struct MyMsg {
    num: usize,
    reply_to: ActorRef<usize>,
}

impl Actor<MyMsg> for MyActor {
    fn receive(&mut self, msg: MyMsg, _context: &mut ActorContext<MyMsg>) {
        msg.reply_to.tell(msg.num);
    }
}

#[test]
fn basic_test() {
    struct TestReaper;

    impl Actor<()> for TestReaper {
        fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
            if let Signal::Started = signal {
                let mut probe = ctx.spawn_probe();

                let actor = ctx.spawn(MyActor);

                // Create an actor ref that can receive usize, and turn them into bool
                // that our probe understands
                let probe_recv = probe
                    .actor_ref
                    .convert(|n| n != 0);
                // Create an actor ref that can receive bool, and turn them into usize
                // that our actor understands
                let actor_send = actor.convert(move |n: bool| {
                    if n {
                        MyMsg {
                            num: 1,
                            reply_to: probe_recv.clone(),
                        }
                    } else {
                        MyMsg {
                            num: 0,
                            reply_to: probe_recv.clone(),
                        }
                    }
                });

                // Tell our actor to tell our probe a result
                actor_send.tell(true);

                // Our probe should get a true
                assert!(probe.receive(Duration::from_secs(10)));

                // And the inverse

                actor_send.tell(false);
                assert!(!probe.receive(Duration::from_secs(10)));

                ctx.actor_ref().drain();
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
