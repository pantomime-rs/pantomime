use crate::actor::*;
use std::time::Duration;

#[test]
fn basic_test() {
    enum State {
        One,
        Two,
        Three,
    }

    struct MyTestActor {
        state: State,
        num: usize,
    };

    impl Actor for MyTestActor {
        type Msg = usize;

        fn receive(&mut self, msg: usize, ctx: &mut ActorContext<usize>) {
            match self.state {
                State::One => {
                    self.num += msg;

                    if self.num == 3 {
                        ctx.schedule_periodic_delivery(
                            "test",
                            Duration::from_millis(200),
                            Duration::from_millis(200),
                            || 111,
                        );

                        self.state = State::Two;
                    }
                }

                State::Two => {
                    assert_eq!(msg, 111);

                    self.num += msg;

                    if self.num == 336 {
                        // 3 in initial state, 3 in final state

                        ctx.cancel_delivery("test");

                        let actor_ref = ctx.actor_ref().clone();

                        ctx.schedule_thunk(Duration::from_millis(500), move || actor_ref.tell(999));

                        self.state = State::Three;
                    }
                }

                State::Three => {
                    assert_eq!(msg, 999);

                    ctx.stop();
                }
            }
        }

        fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<usize>) {
            if let Signal::Started = signal {
                context.schedule_periodic_delivery(
                    "test",
                    Duration::from_millis(200),
                    Duration::from_millis(200),
                    || 1,
                );
            }
        }
    }

    assert!(ActorSystem::new()
        .spawn(MyTestActor {
            state: State::One,
            num: 0
        })
        .is_ok());
}
