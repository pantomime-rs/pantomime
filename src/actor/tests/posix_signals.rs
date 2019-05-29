use crate::actor::*;
use crate::posix_signals;
use std::process;
use std::time::Duration;

struct MyActor;

impl Actor<()> for MyActor {
    fn receive(&mut self, _msg: (), _context: &mut ActorContext<()>) {
        let pid = process::id();

        let killed = process::Command::new("kill")
            .args(&["-s", "hup", &pid.to_string()])
            .status()
            .expect("failed to invoke kill")
            .success();

        assert!(killed);
    }

    fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<()>) {
        match signal {
            Signal::Started => {
                context.watch_posix_signals();

                context.schedule_delivery("kill", Duration::from_millis(100), ());
            }

            Signal::PosixSignal(value) => {
                if value == posix_signals::SIGHUP {
                    context.system_context().stop();
                }
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
                ctx.spawn(MyActor);
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
