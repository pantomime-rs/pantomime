mod tests {
    use crate::actor::*;
    use crate::stream::*;
    use std::time::Duration;

    #[test]
    fn test_sink_for_each_watch_termination() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let mut probe = ctx.spawn_probe::<Option<()>>();
                    let probe_ref = probe.actor_ref.clone();

                    let stream = Sources::iterator(0..=10000)
                        .to(Sinks::for_each(move |_| ()))
                        .watch_termination(move |terminated| match terminated {
                            Terminated::Completed => {
                                probe_ref.tell(Some(()));
                            }

                            Terminated::Failed(_) => {
                                probe_ref.tell(None);
                            }
                        });

                    ctx.spawn_stream(stream);

                    assert_eq!(probe.receive(Duration::from_secs(10)), Some(()));

                    ctx.actor_ref().drain();
                }
            }
        }

        assert!(ActorSystem::spawn(TestReaper).is_ok());
    }

    #[test]
    fn test_sink_last_watch_termination() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let mut probe = ctx.spawn_probe::<Option<usize>>();
                    let probe_ref = probe.actor_ref.clone();

                    let stream = Sources::iterator(1..=10000)
                        .to(Sinks::last())
                        .watch_termination(move |terminated, last| match terminated {
                            Terminated::Completed => {
                                probe_ref.tell(last);
                            }

                            Terminated::Failed(_) => {
                                probe_ref.tell(None);
                            }
                        });

                    ctx.spawn_stream(stream);

                    assert_eq!(probe.receive(Duration::from_secs(10)), Some(10000));

                    ctx.actor_ref().drain();
                }
            }
        }

        assert!(ActorSystem::spawn(TestReaper).is_ok());
    }

    #[test]
    fn test_sink_tell() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let mut probe = ctx
                        .spawn_probe::<TellEvent<usize>>()
                        .with_poll_interval(Duration::from_micros(10));
                    let probe_ref = probe.actor_ref.clone();

                    let stream = Sources::iterator(1..=10000).to(Sinks::tell(probe_ref));

                    ctx.spawn_stream(stream);

                    let mut a = 0;

                    loop {
                        match probe.receive(Duration::from_secs(10)) {
                            TellEvent::Started(handle) => {
                                handle.pull();
                            }

                            TellEvent::Produced(n, handle) => {
                                a += n;
                                handle.pull();
                            }

                            TellEvent::Completed => {
                                break;
                            }

                            _ => (),
                        }
                    }

                    assert_eq!(a, 50_005_000);

                    ctx.actor_ref().drain();
                }
            }
        }

        assert!(ActorSystem::spawn(TestReaper).is_ok());
    }

    #[test]
    fn test_sources_iterator_sink_cancel_watch_termination() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let mut probe = ctx.spawn_probe::<bool>();
                    let probe_ref = probe.actor_ref.clone();

                    let stream = Sources::iterator(1..=10000)
                        .to(Sinks::cancel())
                        .watch_termination(move |terminated| match terminated {
                            Terminated::Completed => {
                                probe_ref.tell(true);
                            }

                            Terminated::Failed(_) => {
                                probe_ref.tell(false);
                            }
                        });

                    ctx.spawn_stream(stream);

                    assert_eq!(probe.receive(Duration::from_secs(10)), true);

                    ctx.actor_ref().drain();
                }
            }
        }

        assert!(ActorSystem::spawn(TestReaper).is_ok());
    }

    #[test]
    fn test_sources_iterator_sink_first_watch_termination() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let mut probe = ctx.spawn_probe::<Option<usize>>();
                    let probe_ref = probe.actor_ref.clone();

                    let stream = Sources::iterator(1..=10000)
                        .to(Sinks::first())
                        .watch_termination(move |terminated, first| match terminated {
                            Terminated::Completed => {
                                probe_ref.tell(first);
                            }

                            Terminated::Failed(_) => {
                                probe_ref.tell(None);
                            }
                        });

                    ctx.spawn_stream(stream);

                    assert_eq!(probe.receive(Duration::from_secs(10)), Some(1));

                    ctx.actor_ref().drain();
                }
            }
        }

        assert!(ActorSystem::spawn(TestReaper).is_ok());
    }

    #[test]
    fn test_sources_empty_sink_last_watch_termination() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let mut probe = ctx.spawn_probe::<Option<usize>>();
                    let probe_ref = probe.actor_ref.clone();

                    let stream = Sources::empty().to(Sinks::last()).watch_termination(
                        move |terminated, last| match terminated {
                            Terminated::Completed => {
                                probe_ref.tell(last);
                            }

                            Terminated::Failed(_) => {
                                probe_ref.tell(None);
                            }
                        },
                    );

                    ctx.spawn_stream(stream);

                    assert_eq!(probe.receive(Duration::from_secs(10)), None);

                    ctx.actor_ref().drain();
                }
            }
        }

        assert!(ActorSystem::spawn(TestReaper).is_ok());
    }

    #[test]
    fn test_sources_iter_flow_take_while_sink_last_watch_termination() {
        use std::u64;

        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let mut probe = ctx.spawn_probe::<Option<u64>>();
                    let probe_ref = probe.actor_ref.clone();

                    let stream = Sources::iterator(1..=u64::MAX)
                        .take_while(|n: &u64| *n <= 10000)
                        .to(Sinks::last())
                        .watch_termination(move |terminated, last| match terminated {
                            Terminated::Completed => {
                                probe_ref.tell(last);
                            }

                            Terminated::Failed(_) => {
                                probe_ref.tell(None);
                            }
                        });

                    ctx.spawn_stream(stream);

                    assert_eq!(probe.receive(Duration::from_secs(10)), Some(10000));

                    ctx.actor_ref().drain();
                }
            }
        }

        assert!(ActorSystem::spawn(TestReaper).is_ok());
    }

    #[test]
    fn test_many() {
        use std::u64;

        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let mut probe = ctx.spawn_probe::<Option<u64>>();
                    let probe_ref = probe.actor_ref.clone();

                    let stream = Sources::iterator(1..=u64::MAX)
                        .take_while(|n: &u64| *n <= 10_000_000)
                        .map(|n| n * 2)
                        .filter(|n| n % 27 == 0)
                        .filter_map(|n| if n % 13 == 0 { Some(n * 3) } else { None })
                        .to(Sinks::last())
                        .watch_termination(move |terminated, last| match terminated {
                            Terminated::Completed => {
                                probe_ref.tell(last);
                            }

                            Terminated::Failed(_) => {
                                probe_ref.tell(None);
                            }
                        });

                    ctx.spawn_stream(stream);

                    assert_eq!(probe.receive(Duration::from_secs(10)), Some(59999940));

                    ctx.actor_ref().drain();
                }
            }
        }

        assert!(ActorSystem::spawn(TestReaper).is_ok());
    }
}
