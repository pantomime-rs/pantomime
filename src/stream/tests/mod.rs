use crate::actor::*;
use crate::stream::*;
use std::time::Duration;

#[test]
fn test1() {
    use crate::actor::*;
    use crate::stream::flow::Delay;
    use std::task::Poll;

    fn double_slowly(n: u64) -> u64 {
        let start = std::time::Instant::now();

        while start.elapsed().as_millis() < 250 {}

        n * 2
    }

    struct TestReaper {
        n: usize,
    }

    impl TestReaper {
        fn new() -> Self {
            Self { n: 0 }
        }
    }

    impl Actor for TestReaper {
        type Msg = ();

        fn receive(&mut self, _: (), ctx: &mut ActorContext<()>) {
            self.n += 1;

            println!("n={}", self.n);

            if self.n == 2 {
                ctx.stop();
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
            match signal {
                Signal::Started => {
                    // @TODO this fails to complete due to a bug...

                    let (stream_ref, result) = ctx.spawn(
                        Source::iterator(1..=20)
                            .via(Flow::new(Delay::new(Duration::from_millis(50))))
                            .via(Flow::new(Delay::new(Duration::from_millis(500))))
                            .to(Sink::for_each(|n| println!("got {}", n))),
                    );

                    ctx.watch(stream_ref, |_: StopReason| ());
                    ctx.watch(result, |value| value);
                }

                _ => {}
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}

#[test]
fn test2() {
    use crate::actor::*;
    use crate::stream::flow::Delay;
    use std::io::{Error, ErrorKind};

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

        fn receive(&mut self, value: usize, ctx: &mut ActorContext<usize>) {
            self.n += value;

            if self.n == 101 {
                ctx.stop();
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<usize>) {
            match signal {
                Signal::Started => {
                    {
                        let actor_ref = ctx.actor_ref().clone();

                        ctx.schedule_thunk(Duration::from_secs(10), move || {
                            actor_ref
                                .fail(FailureError::new(Error::new(ErrorKind::Other, "failed")))
                        });
                    }

                    let (stream_ref, result) = ctx.spawn(
                        Source::iterator(1..=100_000_000)
                            .via(Flow::new(Delay::new(Duration::from_millis(50))))
                            .to(Sink::first()),
                    );

                    ctx.watch(stream_ref, |_: StopReason| 100);
                    ctx.watch(result, |value: Option<usize>| value.unwrap_or_default());
                }

                _ => {}
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}

#[test]
fn test3() {
    use crate::actor::*;
    use crate::stream::flow::Delay;
    use std::io::{Error, ErrorKind};

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

        fn receive(&mut self, value: usize, ctx: &mut ActorContext<usize>) {
            self.n += value;

            if self.n == 6 {
                // 0 + 1 -> 1
                // 1 + 2 -> 3
                // 3 + 3 -> 6

                ctx.stop();
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<usize>) {
            match signal {
                Signal::Started => {
                    {
                        let actor_ref = ctx.actor_ref().clone();

                        ctx.schedule_thunk(Duration::from_secs(10), move || {
                            actor_ref
                                .fail(FailureError::new(Error::new(ErrorKind::Other, "failed")))
                        });
                    }

                    let (_, result) = ctx.spawn(
                        Source::iterator(1..=3)
                            .via(Flow::scan(0, |last, next| last + next))
                            .to(Sink::last()),
                    );

                    ctx.watch(result, |value: Option<usize>| value.unwrap_or_default());
                }

                _ => {}
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}

#[test]
fn test4() {
    use crate::actor::*;
    use crate::stream::flow::Delay;
    use std::io::{Error, ErrorKind};

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

        fn receive(&mut self, value: usize, ctx: &mut ActorContext<usize>) {
            self.n += value;

            if self.n == 465 {
                ctx.stop();
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<usize>) {
            match signal {
                Signal::Started => {
                    {
                        let actor_ref = ctx.actor_ref().clone();

                        ctx.schedule_thunk(Duration::from_secs(10), move || {
                            actor_ref
                                .fail(FailureError::new(Error::new(ErrorKind::Other, "failed")))
                        });
                    }

                    let a = Source::iterator(1..=10);
                    let b = Source::iterator(11..=20);
                    let c = Source::iterator(21..=30);

                    let (_, result) = ctx.spawn(
                        a.merge(b)
                            .merge(c)
                            .via(Flow::scan(0, |last, next| last + next))
                            .via(Flow::new(Delay::new(Duration::from_millis(50))))
                            .to(Sink::last()),
                    );

                    ctx.watch(result, |value: Option<usize>| value.unwrap_or_default());
                }

                _ => {}
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}
