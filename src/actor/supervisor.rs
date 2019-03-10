use super::*;
use std::panic;

enum Strategy {
    Resume,
}

pub struct Supervisor<M: 'static + Send, A>
where
    A: Actor<M>,
{
    actor: A,
    strategy: Strategy,
    on_resume: Option<Box<Fn() -> M + Send>>,
}

impl<M: 'static + Send, A> Supervisor<M, A>
where
    A: Actor<M>,
{
    /// Supervision strategy that simply resumes on failure.
    ///
    /// If an actor panics when receiving a message or signal,
    /// this simply discards the panic and continues.
    ///
    /// This requires the Actor to be unwind-safe, which most
    /// code is.
    ///
    /// @TODO use the types to ensure above?
    pub fn resume(actor: A) -> Self {
        Self {
            actor,
            strategy: Strategy::Resume,
            on_resume: None,
        }
    }

    /// Specify a message to send when the stategy has resumed
    /// execution of an actor.
    ///
    /// This is guaranteed to be sent before any other messages.
    pub fn on_resume<F: Fn() -> M>(mut self, f: F) -> Self
    where
        F: 'static + Send,
    {
        self.on_resume = Some(Box::new(f));
        self
    }

    fn failed(&mut self, context: &mut ActorContext<M>) {
        match &self.strategy {
            Strategy::Resume => {
                if let Some(ref f) = self.on_resume {
                    self.receive(f(), context);
                }
            }
        }
    }
}

impl<M: 'static + Send, A> Actor<M> for Supervisor<M, A>
where
    A: Actor<M>,
{
    fn receive(&mut self, message: M, context: &mut ActorContext<M>) {
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            self.actor.receive(message, context);
        }));

        if result.is_err() {
            self.failed(context);
        }
    }

    fn receive_signal(&mut self, message: Signal, context: &mut ActorContext<M>) {
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            self.actor.receive_signal(message, context);
        }));

        if result.is_err() {
            self.failed(context);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time;

    struct Divider;

    enum DividerMsg {
        Divide(usize, usize, ActorRef<usize>),
    }

    impl Actor<DividerMsg> for Divider {
        fn receive(&mut self, msg: DividerMsg, _context: &mut ActorContext<DividerMsg>) {
            match msg {
                DividerMsg::Divide(num, den, reply_to) => {
                    reply_to.tell(num / den);
                }
            }
        }
    }

    #[test]
    fn test_supervisor_strategy_resume() {
        let mut system = ActorSystem::new().start();

        let mut probe = system.spawn_probe::<usize>();

        let divider = {
            let probe_ref = probe.actor_ref.clone();

            system.spawn(
                Supervisor::resume(Divider)
                    .on_resume(move || DividerMsg::Divide(32, 2, probe_ref.clone())),
            )
        };

        divider.tell(DividerMsg::Divide(4, 0, probe.actor_ref.clone()));

        assert_eq!(probe.receive(time::Duration::from_secs(10)), 16);

        divider.tell(DividerMsg::Divide(4, 1, probe.actor_ref.clone()));

        assert_eq!(probe.receive(time::Duration::from_secs(10)), 4);

        system.context.drain();

        system.join();
    }

    // @TODO test failures in signal handling
}
