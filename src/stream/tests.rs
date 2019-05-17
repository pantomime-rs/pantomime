mod tests {
    use crate::actor::*;
    use crate::stream::*;
    use std::time::Duration;

    #[test]
    fn test_sink_for_each_watch_termination() {
        let mut system = ActorSystem::new().start();
        let mut probe = system.spawn_probe::<Option<()>>();
        let probe_ref = probe.actor_ref.clone();

        Sources::iterator(0..=10000)
            .to(Sinks::for_each(move |_| ()))
            .watch_termination(move |terminated| match terminated {
                Terminated::Completed => {
                    probe_ref.tell(Some(()));
                }

                Terminated::Failed(_) => {
                    probe_ref.tell(None);
                }
            })
            .run(&system.context);

        assert_eq!(probe.receive(Duration::from_secs(10)), Some(()));

        system.context.stop();
        system.join();
    }

    #[test]
    fn test_sink_last_watch_termination() {
        let mut system = ActorSystem::new().start();
        let mut probe = system.spawn_probe::<Option<usize>>();
        let probe_ref = probe.actor_ref.clone();

        Sources::iterator(1..=10000)
            .to(Sinks::last())
            .watch_termination(move |terminated, last| match terminated {
                Terminated::Completed => {
                    probe_ref.tell(last);
                }

                Terminated::Failed(_) => {
                    probe_ref.tell(None);
                }
            })
            .run(&system.context);

        assert_eq!(probe.receive(Duration::from_secs(10)), Some(10000));

        system.context.stop();
        system.join();
    }

    #[test]
    fn test_sink_tell() {
        let mut system = ActorSystem::new().start();
        let mut probe = system
            .spawn_probe::<TellEvent<usize>>()
            .with_poll_interval(Duration::from_micros(10));
        let probe_ref = probe.actor_ref.clone();

        Sources::iterator(1..=10000)
            .to(Sinks::tell(probe_ref))
            .run(&system.context);

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

                _ => ()
            }
        }

        assert_eq!(a, 50_005_000);

        system.context.stop();
        system.join();
    }

    #[test]
    fn test_sources_iterator_sink_cancel_watch_termination() {
        let mut system = ActorSystem::new().start();
        let mut probe = system.spawn_probe::<bool>();
        let probe_ref = probe.actor_ref.clone();

        Sources::iterator(1..=10000)
            .to(Sinks::cancel())
            .watch_termination(move |terminated| match terminated {
                Terminated::Completed => {
                    probe_ref.tell(true);
                }

                Terminated::Failed(_) => {
                    probe_ref.tell(false);
                }
            })
            .run(&system.context);

        assert_eq!(probe.receive(Duration::from_secs(10)), true);

        system.context.stop();
        system.join();
    }

    #[test]
    fn test_sources_iterator_sink_first_watch_termination() {
        let mut system = ActorSystem::new().start();
        let mut probe = system.spawn_probe::<Option<usize>>();
        let probe_ref = probe.actor_ref.clone();

        Sources::iterator(1..=10000)
            .to(Sinks::first())
            .watch_termination(move |terminated, first| match terminated {
                Terminated::Completed => {
                    probe_ref.tell(first);
                }

                Terminated::Failed(_) => {
                    probe_ref.tell(None);
                }
            })
            .run(&system.context);

        assert_eq!(probe.receive(Duration::from_secs(10)), Some(1));

        system.context.stop();
        system.join();
    }

    #[test]
    fn test_sources_empty_sink_last_watch_termination() {
        let mut system = ActorSystem::new().start();
        let mut probe = system.spawn_probe::<Option<usize>>();
        let probe_ref = probe.actor_ref.clone();

        Sources::empty()
            .to(Sinks::last())
            .watch_termination(move |terminated, last| match terminated {
                Terminated::Completed => {
                    probe_ref.tell(last);
                }

                Terminated::Failed(_) => {
                    probe_ref.tell(None);
                }
            })
            .run(&system.context);

        assert_eq!(probe.receive(Duration::from_secs(10)), None);

        system.context.stop();
        system.join();
    }

    #[test]
    fn test_sources_iter_flow_take_while_sink_last_watch_termination() {
        use std::u64;

        let mut system = ActorSystem::new().start();
        let mut probe = system.spawn_probe::<Option<u64>>();
        let probe_ref = probe.actor_ref.clone();

        Sources::iterator(1..=u64::MAX)
            .take_while(|n: &u64| *n <= 10000)
            .to(Sinks::last())
            .watch_termination(move |terminated, last| match terminated {
                Terminated::Completed => {
                    probe_ref.tell(last);
                }

                Terminated::Failed(_) => {
                    probe_ref.tell(None);
                }
            })
            .run(&system.context);

        assert_eq!(probe.receive(Duration::from_secs(10)), Some(10000));

        system.context.stop();
        system.join();
    }

    #[test]
    fn test_many() {
        use std::u64;

        let mut system = ActorSystem::new().start();
        let mut probe = system.spawn_probe::<Option<u64>>();
        let probe_ref = probe.actor_ref.clone();

        Sources::iterator(1..=u64::MAX)
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
            })
            .run(&system.context);

        assert_eq!(probe.receive(Duration::from_secs(10)), Some(59999940));

        system.context.stop();
        system.join();
    }

    #[test]
    fn test_merge() {
        return;

        let mut system = ActorSystem::new().start();

        let a = Sources::iterator(0..=99);
        let b = Sources::iterator(100..=199);

        println!("yea!");

        a.merge(b).run_with(Sinks::for_each(|n| {
            println!("received: {}", n);
        }), &system.context);


        //system.context.stop();
        system.join();
    }
}
