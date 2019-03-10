mod test_actor_convert {
    use crate::actor::*;
    use std::time::Duration;

    struct MyActor;

    struct MyMsg {
        num: usize,
        reply_to: ActorRef<usize>,
    }

    impl Actor<MyMsg> for MyActor {
        fn receive(&mut self, msg: MyMsg, _context: &mut ActorContext<MyMsg>) {
            msg.reply_to.tell(msg.num);
        }
    }

    #[test]
    fn basic_test() {
        let mut system = ActorSystem::new().start();

        let mut probe = system.spawn_probe::<bool>();

        let actor = system.spawn(MyActor);

        // Create an actor ref that can receive usize, and turn them into bool
        // that our probe understands
        let probe_recv = probe
            .actor_ref
            .convert(|n| if n == 0 { false } else { true });

        // Create an actor ref that can receive bool, and turn them into usize
        // that our actor understands
        let actor_send = actor.convert(move |n: bool| {
            if n {
                MyMsg {
                    num: 1,
                    reply_to: probe_recv.clone(),
                }
            } else {
                MyMsg {
                    num: 0,
                    reply_to: probe_recv.clone(),
                }
            }
        });

        // Tell our actor to tell our probe a result
        actor_send.tell(true);

        // Our probe should get a true
        assert!(probe.receive(Duration::from_secs(10)));

        // And the inverse

        actor_send.tell(false);
        assert!(!probe.receive(Duration::from_secs(10)));

        // Dispatch shutdown
        system.context.stop();

        // Ensure that system messages (e.g. shutdown) are handled
        system.join();
    }
}

mod test_actor_drain {
    use crate::actor::*;
    use std::time::Duration;

    const LIMIT: usize = 1024;
    const TIMES: usize = 128;

    struct MyActor {
        id: usize,
        actor_ref: ActorRef<usize>,
        count: usize,
    }

    impl Actor<usize> for MyActor {
        fn receive(&mut self, msg: usize, _context: &mut ActorContext<usize>) {
            if self.id != 0 {
                self.actor_ref.tell(msg);
            } else {
                self.count += msg;

                if self.count == LIMIT * TIMES {
                    self.actor_ref.tell(self.count);
                }
            }
        }

        fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<usize>) {
            match signal {
                Signal::Started => {
                    if self.id == 0 {
                        for id in 1..LIMIT + 1 {
                            let a = context.spawn(MyActor {
                                id,
                                actor_ref: context.actor_ref().clone(),
                                count: 0,
                            });

                            for _ in 0..TIMES {
                                a.tell(1);
                            }

                            a.drain();
                        }
                    }
                }

                Signal::Stopped => {
                    if self.id == 0 {
                        self.actor_ref.tell(self.count);
                    }
                }

                _ => (),
            }
        }
    }

    #[test]
    fn test() {
        let mut system = ActorSystem::new().start();

        let mut probe = system.spawn_probe::<usize>();

        system.spawn(MyActor {
            id: 0,
            actor_ref: probe.actor_ref.clone(),
            count: 0,
        });

        assert_eq!(probe.receive(Duration::from_secs(10)), LIMIT * TIMES);

        system.context.drain();

        system.join();
    }
}

#[cfg(feature = "posix-signals-support")]
mod test_posix_signals {
    use crate::actor::*;
    use crate::posix_signals;
    use std::process;
    use std::time::Duration;

    struct MyActor;

    impl Actor<()> for MyActor {
        fn receive(&mut self, _msg: (), _context: &mut ActorContext<()>) {
            let pid = process::id();

            let killed = process::Command::new("kill")
                .args(&["-s", &posix_signals::SIGUSR1.to_string(), &pid.to_string()])
                .status()
                .expect("failed to invoke kill")
                .success();

            assert!(killed);
        }

        fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<()>) {
            match signal {
                Signal::Started => {
                    context.watch_posix_signals();

                    context.schedule_delivery("kill", Duration::from_millis(100), || ());
                }

                Signal::PosixSignal(value) => {
                    if value == posix_signals::SIGUSR1 {
                        context.actor_ref().system_context().drain();
                    }
                }

                _ => {}
            }
        }
    }

    #[test]
    #[cfg(not(windows))]
    fn basic_test() {
        let mut system = ActorSystem::new().start();

        system.spawn(MyActor);

        system.join();
    }
}
