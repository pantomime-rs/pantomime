use crate::actor::{ActorContext, Watchable};

pub enum PosixSignal {
    SIGHUP = 1,
    SIGINT = 2,
    SIGTERM = 15,
}

/// When watched, register interest in receiving POSIX signals. When
/// the process receives a POSIX signal, it will be converted
/// via the supplied function into a message the actor can receive.
///
/// Usage is often performed from the root reaper actor, but any
/// actor in the system is eligible to watch the signals.
///
/// POSIX signals are not supported on Windows and thus no action
/// will be performed when running on Windows systems.
pub struct PosixSignals;

impl<Msg, F: Fn(PosixSignal) -> Msg> Watchable<PosixSignals, PosixSignal, Msg, F>
    for ActorContext<Msg>
where
    Msg: 'static + Send,
    F: 'static + Send,
{
    fn perform_watch(&mut self, _: PosixSignals, convert: F) {
        self.watch_posix_signals_with(convert);
    }
}
