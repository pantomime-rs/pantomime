use crate::actor::*;
use std::io;

struct MyActor {
    fail: bool,
}

impl Actor<()> for MyActor {
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
        Stopped,
        Failed,
    }

    enum ReaperState {
        One,
        Two,
        Three,
        Four,
    }

    struct TestReaper {
        state: ReaperState,
    }

    impl Actor<ReaperMsg> for TestReaper {
        fn receive(&mut self, msg: ReaperMsg, ctx: &mut ActorContext<ReaperMsg>) {
            match self.state {
                ReaperState::One => {
                    panic!("unexpected msg in One");
                }

                ReaperState::Two => {
                    panic!("unexpected msg in Two");
                }

                ReaperState::Three => match msg {
                    ReaperMsg::Failed => {
                        self.state = ReaperState::Four;

                        ctx.spawn_watched_with(MyActor { fail: false }, |reason| match reason {
                            StopReason::Stopped => ReaperMsg::Stopped,
                            StopReason::Failed => ReaperMsg::Failed,
                        })
                        .tell(());
                    }

                    ReaperMsg::Stopped => {
                        panic!("unexpected msg in Three");
                    }
                },

                ReaperState::Four => match msg {
                    ReaperMsg::Stopped => {
                        ctx.stop();
                    }

                    ReaperMsg::Failed => {
                        panic!("unexpected msg in Four");
                    }
                },
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<ReaperMsg>) {
            match self.state {
                ReaperState::One => match signal {
                    Signal::Started => {
                        ctx.spawn_watched(MyActor { fail: true }).tell(());
                    }

                    Signal::ActorStopped(_, StopReason::Failed) => {
                        self.state = ReaperState::Two;

                        ctx.spawn_watched(MyActor { fail: false }).tell(());
                    }

                    _ => {
                        panic!("unexpected signal in One");
                    }
                },

                ReaperState::Two => match signal {
                    Signal::ActorStopped(_, StopReason::Stopped) => {
                        self.state = ReaperState::Three;

                        ctx.spawn_watched_with(MyActor { fail: true }, |reason| match reason {
                            StopReason::Stopped => ReaperMsg::Stopped,
                            StopReason::Failed => ReaperMsg::Failed,
                        })
                        .tell(());
                    }

                    _ => {
                        panic!("unexpected signal in One");
                    }
                },

                ReaperState::Three => {}

                ReaperState::Four => {}
            }
        }
    }

    assert!(ActorSystem::new()
        .spawn(TestReaper {
            state: ReaperState::One
        })
        .is_ok());
}
