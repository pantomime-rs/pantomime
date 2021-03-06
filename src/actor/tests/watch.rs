use crate::actor::*;
use std::io;

struct MyActor {
    fail: bool,
}

impl Actor for MyActor {
    type Msg = ();

    fn receive(&mut self, _: (), ctx: &mut ActorContext<()>) {
        if self.fail {
            ctx.fail(io::Error::new(io::ErrorKind::Other, "failed"));
        } else {
            ctx.stop();
        }
    }
}

#[test]
#[cfg(not(windows))]
fn basic_test() {
    enum ReaperMsg {
        FirstChildStopped(StopReason),
        SecondChildStopped(StopReason),
        Stopped,
        Failed,
    }

    enum ReaperState {
        One,
        Two,
        Three,
        Four,
        Five,
    }

    struct TestReaper {
        state: ReaperState,
    }

    impl Actor for TestReaper {
        type Msg = ReaperMsg;

        fn receive(&mut self, msg: ReaperMsg, ctx: &mut ActorContext<ReaperMsg>) {
            match self.state {
                ReaperState::One => match msg {
                    ReaperMsg::FirstChildStopped(StopReason::Failed) => {
                        self.state = ReaperState::Two;

                        let actor_ref = ctx.spawn(MyActor { fail: false });

                        ctx.watch(&actor_ref, ReaperMsg::SecondChildStopped);

                        actor_ref.tell(());
                    }

                    _ => {
                        panic!("unexpected msg in One");
                    }
                },

                ReaperState::Two => match msg {
                    ReaperMsg::SecondChildStopped(StopReason::Stopped) => {
                        self.state = ReaperState::Three;

                        let actor_ref = ctx.spawn(MyActor { fail: true });

                        ctx.watch(&actor_ref, |reason| match reason {
                            StopReason::Stopped => ReaperMsg::Stopped,
                            StopReason::Failed => ReaperMsg::Failed,
                        });

                        actor_ref.tell(());
                    }

                    _ => {
                        panic!("unexpected msg in Two");
                    }
                },

                ReaperState::Three => match msg {
                    ReaperMsg::Failed => {
                        self.state = ReaperState::Four;

                        let actor_ref = ctx.spawn(MyActor { fail: false });

                        ctx.watch(&actor_ref, |reason| match reason {
                            StopReason::Stopped => ReaperMsg::Stopped,
                            StopReason::Failed => ReaperMsg::Failed,
                        });

                        actor_ref.tell(());
                    }

                    _ => {
                        panic!("unexpected msg in Three");
                    }
                },

                ReaperState::Four => match msg {
                    ReaperMsg::Stopped => {
                        self.state = ReaperState::Five;

                        let actor_ref = ctx.spawn(MyActor { fail: false });

                        ctx.watch(&actor_ref, |_| panic!("failure to convert"));

                        actor_ref.tell(());
                    }

                    _ => {
                        panic!("unexpected msg in Four");
                    }
                },

                ReaperState::Five => panic!("unexpected msg in Five"),
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<ReaperMsg>) {
            if let ReaperState::One = self.state {
                match signal {
                    Signal::Started => {
                        let actor_ref = ctx.spawn(MyActor { fail: true });

                        ctx.watch(&actor_ref, ReaperMsg::FirstChildStopped);

                        actor_ref.tell(());
                    }

                    _ => {
                        panic!("unexpected signal in One");
                    }
                }
            }
        }

        fn handle_failure(
            &mut self,
            _reason: FailureReason,
            ctx: &mut ActorContext<Self::Msg>,
        ) -> FailureAction {
            ctx.stop();

            FailureAction::Resume
        }
    }

    assert!(ActorSystem::new()
        .spawn(TestReaper {
            state: ReaperState::One
        })
        .is_ok());
}
