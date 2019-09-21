use crate::actor::*;
use std::time::Duration;

enum MyMsg {
    Fail,
    Increment,
    Get,
}

struct MyActor {
    id: usize,
    num: usize,
    signalled: bool,
    actor_ref: ActorRef<usize>,
}

impl Actor<MyMsg> for MyActor {
    fn handle_failure(&mut self, r: FailureReason, _: &mut ActorContext<MyMsg>) -> FailureAction {
        match self.id {
            0 => FailureAction::Fail(r),
            1 => FailureAction::Resume,
            _ => FailureAction::Fail(r),
        }
    }

    fn receive_signal(&mut self, signal: Signal, _: &mut ActorContext<MyMsg>) {
        match (self.id, signal) {
            (0, Signal::Stopped(Some(_))) => {
                self.actor_ref.tell(self.num);
            }

            (1, Signal::Resumed) => {
                self.signalled = true;
            }

            _ => {}
        }
    }

    fn receive(&mut self, msg: MyMsg, _: &mut ActorContext<MyMsg>) {
        match msg {
            MyMsg::Get => {
                if self.signalled {
                    self.actor_ref.tell(self.num);
                }
            }

            MyMsg::Increment => {
                self.num += 1;
            }

            MyMsg::Fail => {
                panic!();
            }
        }
    }
}

#[test]
fn test() {
    struct TestReaper;

    impl Actor<()> for TestReaper {
        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
            if let Signal::Started = signal {
                let mut probe = ctx.spawn_probe::<usize>();

                let fail_ref = ctx.spawn(MyActor {
                    id: 0,
                    num: 0,
                    signalled: false,
                    actor_ref: probe.actor_ref().clone(),
                });
                fail_ref.tell(MyMsg::Increment);
                fail_ref.tell(MyMsg::Increment);
                fail_ref.tell(MyMsg::Fail);
                assert_eq!(probe.receive(Duration::from_secs(10)), 2);

                let resume_ref = ctx.spawn(MyActor {
                    id: 1,
                    num: 0,
                    signalled: false,
                    actor_ref: probe.actor_ref().clone(),
                });

                resume_ref.tell(MyMsg::Increment);
                resume_ref.tell(MyMsg::Increment);
                resume_ref.tell(MyMsg::Increment);
                resume_ref.tell(MyMsg::Increment);
                resume_ref.tell(MyMsg::Fail);
                resume_ref.tell(MyMsg::Get);

                assert_eq!(probe.receive(Duration::from_secs(10)), 4);

                ctx.actor_ref().stop();
            }
        }

        fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
