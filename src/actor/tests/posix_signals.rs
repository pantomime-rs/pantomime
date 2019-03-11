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
