use crate::actor::*;
use crate::posix_signals::{PosixSignal, PosixSignals};
use std::process;
use std::time::Duration;

enum MyMsg {
    Signal1(PosixSignal),
    Signal2(PosixSignal),
    DoKill,
}

struct MyActor {
    received_signal1: bool,
}

impl Actor<MyMsg> for MyActor {
    fn receive(&mut self, msg: MyMsg, context: &mut ActorContext<MyMsg>) {
        match msg {
            MyMsg::DoKill => {
                let pid = process::id();

                let killed = process::Command::new("kill")
                    .args(&["-s", "hup", &pid.to_string()])
                    .status()
                    .expect("failed to invoke kill")
                    .success();

                assert!(killed);
            }

            MyMsg::Signal1(PosixSignal::SIGHUP) => {
                self.received_signal1 = true;
            }

            MyMsg::Signal2(PosixSignal::SIGHUP) if self.received_signal1 => {
                context.system_context().stop();
            }

            _ => {}
        }
    }

    fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<MyMsg>) {
        match signal {
            Signal::Started => {
                context.watch(PosixSignals, MyMsg::Signal1);
                context.watch(PosixSignals, MyMsg::Signal2);

                context.schedule_delivery("kill", Duration::from_millis(100), MyMsg::DoKill);
            }

            _ => {}
        }
    }
}

#[test]
#[cfg(not(windows))]
fn basic_test() {
    struct TestReaper;

    impl Actor<()> for TestReaper {
        fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
            if let Signal::Started = signal {
                ctx.spawn(MyActor {
                    received_signal1: false,
                });
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
