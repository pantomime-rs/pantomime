use crate::stream::{Action, Logic, LogicEvent, StreamContext};

enum DelayMsg<A> {
    Ready(A),
}

struct TestLogic {
    state: State,
    fusible: bool,
}

enum State {
    Waiting,
    Pushing,
    Stopping,
}

impl<A: Send> Logic<A, A> for TestLogic {
    type Ctl = DelayMsg<A>;

    fn name(&self) -> &'static str {
        "TestLogic"
    }

    fn buffer_size(&self) -> Option<usize> {
        Some(0)
    }

    fn fusible(&self) -> bool {
        self.fusible
    }

    fn receive(
        &mut self,
        msg: LogicEvent<A, Self::Ctl>,
        ctx: &mut StreamContext<A, A, Self::Ctl>,
    ) -> Action<A, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => Action::Pull,

            LogicEvent::Pushed(element) => {
                let stream_ref = ctx.stage_ref();

                std::thread::spawn(move || {
                    stream_ref.tell(DelayMsg::Ready(element));
                });

                self.state = State::Pushing;

                Action::None
            }

            LogicEvent::Started => Action::None,

            LogicEvent::Stopped => match self.state {
                State::Pushing => {
                    self.state = State::Stopping;

                    Action::None
                }

                State::Waiting => Action::Stop(None),

                State::Stopping => Action::None,
            },

            LogicEvent::Cancelled => Action::Stop(None),

            LogicEvent::Forwarded(DelayMsg::Ready(element)) => {
                self.state = State::Waiting;

                if let State::Stopping = self.state {
                    Action::PushAndStop(element, None)
                } else {
                    Action::Push(element)
                }
            }
        }
    }
}

#[test]
fn test() {
    use crate::actor::*;
    use crate::stream::{Flow, Sink, Source};

    struct TestReaper {
        n: usize,
    }

    impl TestReaper {
        fn new() -> Self {
            Self { n: 0 }
        }
    }

    impl Actor for TestReaper {
        type Msg = usize;

        fn receive(&mut self, n: Self::Msg, ctx: &mut ActorContext<Self::Msg>) {
            self.n += n;

            if self.n == 12 {
                ctx.stop();
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<Self::Msg>) {
            if let Signal::Started = signal {
                let actor_ref = ctx.actor_ref().clone();

                ctx.spawn(
                    Source::iterator(1..=3)
                        .via(Flow::from_logic(TestLogic {
                            state: State::Waiting,
                            fusible: false,
                        }))
                        .to(Sink::for_each(move |n| actor_ref.tell(n))),
                );

                let actor_ref = ctx.actor_ref().clone();

                ctx.spawn(
                    Source::iterator(1..=3)
                        .via(Flow::from_logic(TestLogic {
                            state: State::Waiting,
                            fusible: true,
                        }))
                        .to(Sink::for_each(move |n| actor_ref.tell(n))),
                );
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}
