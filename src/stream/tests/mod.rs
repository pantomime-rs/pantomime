use crate::stream::*;
use std::time::Duration;

#[test]
fn test1() {
    use crate::actor::*;
    use crate::stream::flow::Delay;

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

            if self.n == 2 {
                ctx.stop();
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
            if let Signal::Started = signal {
                // @TODO this fails to complete due to a bug...

                let (stream_ref, result) = ctx.spawn(
                    Source::iterator(1..=10)
                        .via(Flow::from_logic(Delay::new(Duration::from_millis(50))))
                        .via(Flow::from_logic(Delay::new(Duration::from_millis(500))))
                        .to(Sink::for_each(|_| ())),
                );

                ctx.watch(stream_ref, |_: StopReason| ());
                ctx.watch(result, |value| value);
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
            if let Signal::Started = signal {
                {
                    let actor_ref = ctx.actor_ref().clone();

                    ctx.schedule_thunk(Duration::from_secs(10), move || {
                        actor_ref.fail(FailureError::new(Error::new(ErrorKind::Other, "failed")))
                    });
                }

                let (stream_ref, result) = ctx.spawn(
                    Source::iterator(1..100_000_000)
                        .via(Flow::from_logic(Delay::new(Duration::from_millis(50))))
                        .to(Sink::first()),
                );

                ctx.watch(stream_ref, |_: StopReason| 100);
                ctx.watch(result, |value: Option<usize>| value.unwrap_or_default());
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}

#[test]
fn test3() {
    // @TODO fix this test

    /*
    use crate::actor::*;
    use std::io::{Error, ErrorKind};

    struct TestReaper {
        n: usize,
        #[allow(dead_code)] // @TODO
        started: std::time::Instant,
    }

    impl TestReaper {
        fn new() -> Self {
            Self {
                n: 0,
                started: std::time::Instant::now(),
            }
        }
    }

    impl Actor for TestReaper {
        type Msg = usize;

        fn receive(&mut self, value: usize, ctx: &mut ActorContext<usize>) {
            self.n += value;

            //println!("       N IS  {}", self.n);
            if self.n == 6 {
                // 0 + 1 -> 1
                // 1 + 2 -> 3
                // 3 + 3 -> 6

                // @TODO
                //println!("stopping! (took {:?})", self.started.elapsed());
                ctx.stop();
            } else {
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<usize>) {
            if let Signal::Started = signal {
                    if true {
                        {
                            let actor_ref = ctx.actor_ref().clone();

                            ctx.schedule_thunk(Duration::from_secs(100), move || {
                                actor_ref
                                    .fail(FailureError::new(Error::new(ErrorKind::Other, "failed")))
                            });
                        }

                        let (_, result) = ctx.spawn(
                            Source::iterator(1..=10_000_000)
                                //.map(|n| n)
                                //.map(|n| n)
                                //.map(|n| n)
                                /*.via(Flow::from_logic(Delay::new(Duration::from_millis(1))))
                                .map(|n| {
                                    println!("one: {}", n);
                                    n * 2
                                })
                                .via(Flow::from_logic(Delay::new(Duration::from_millis(1))))
                                .map(|n| {
                                    println!("two: {}", n);
                                    n * 2
                                })
                                .via(Flow::from_logic(Delay::new(Duration::from_millis(1))))*/
                                .to(Sink::last()),
                        );

                        ctx.watch(result, |value| value.unwrap_or_default());
                    }

                    if false {
                        ctx.actor_ref().tell(6);
                    }
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());

    */
}

#[test]
fn test4() {
    // @TODO fix this test

    /*
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
            if let Signal::Started = signal {
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

                let sink =
                    Flow::from_logic(Delay::new(Duration::from_millis(50))).to(Sink::last());

                let (_, result) = ctx.spawn(
                    a.merge(b)
                        .merge(c)
                        .via(Flow::new().scan(0, |last, next| last + next))
                        .to(sink),
                );

                ctx.watch(result, |value: Option<usize>| value.unwrap_or_default());
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
    */
}
