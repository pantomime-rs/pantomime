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
}
