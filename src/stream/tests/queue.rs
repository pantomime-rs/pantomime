use crate::stream::*;

#[test]
fn test_basic() {
    use crate::actor::*;
    use std::io::{Error, ErrorKind};

    struct TestReaper {
        n: usize,
    };

    impl TestReaper {
        fn new() -> Self {
            Self { n: 0 }
        }
    }

    impl Actor for TestReaper {
        type Msg = Vec<usize>;

        fn receive(&mut self, value: Self::Msg, ctx: &mut ActorContext<Self::Msg>) {
            let sum: usize = value.iter().sum();

            self.n += sum;

            if self.n == 320 {
                ctx.stop();
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<Self::Msg>) {
            if let Signal::Started = signal {
                {
                    let actor_ref = ctx.actor_ref().clone();

                    ctx.schedule_thunk(Duration::from_secs(10), move || {
                        actor_ref.fail(FailureError::new(Error::new(ErrorKind::Other, "failed")))
                    });
                }

                let (queue_ref, queue_src) = ctx.spawn(Source::queue(16));

                // @TODO test dropping behavior
                queue_ref.push(10, ActorRef::empty());
                queue_ref.push(20, ActorRef::empty());
                queue_ref.push(30, ActorRef::empty());
                queue_ref.complete();

                let (stream_ref, result) = ctx.spawn(queue_src.map(|n| n * 2).to(Sink::collect()));

                ctx.watch(result, |value| value);
                ctx.watch(stream_ref, |_: StopReason| vec![200]);
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}

#[test]
fn test_drop_newest() {
    use crate::actor::*;
    use std::io::{Error, ErrorKind};

    struct TestReaper {
        n: usize,
    };

    impl TestReaper {
        fn new() -> Self {
            Self { n: 0 }
        }
    }

    impl Actor for TestReaper {
        type Msg = Vec<usize>;

        fn receive(&mut self, value: Self::Msg, ctx: &mut ActorContext<Self::Msg>) {
            let sum: usize = value.iter().sum();

            self.n += sum;

            if self.n == 320 {
                ctx.stop();
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<Self::Msg>) {
            if let Signal::Started = signal {
                {
                    let actor_ref = ctx.actor_ref().clone();

                    ctx.schedule_thunk(Duration::from_secs(10), move || {
                        actor_ref.fail(FailureError::new(Error::new(ErrorKind::Other, "failed")))
                    });
                }

                let (queue_ref, queue_src) = ctx
                    .spawn(Source::queue(16).with_overflow_strategy(OverflowStrategy::DropNewest));

                // @TODO test dropping behavior
                queue_ref.push(10, ActorRef::empty());
                queue_ref.push(20, ActorRef::empty());
                queue_ref.push(30, ActorRef::empty());
                queue_ref.complete();

                let (stream_ref, result) = ctx.spawn(queue_src.map(|n| n * 2).to(Sink::collect()));

                ctx.watch(result, |value| value);
                ctx.watch(stream_ref, |_: StopReason| vec![200]);
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}
