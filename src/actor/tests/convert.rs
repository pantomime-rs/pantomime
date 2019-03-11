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
    let mut system = ActorSystem::new().start();

    let mut probe = system.spawn_probe::<bool>();

    let actor = system.spawn(MyActor);

    // Create an actor ref that can receive usize, and turn them into bool
    // that our probe understands
    let probe_recv = probe
        .actor_ref
        .convert(|n| if n == 0 { false } else { true });

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

    // Dispatch shutdown
    system.context.stop();

    // Ensure that system messages (e.g. shutdown) are handled
    system.join();
}
