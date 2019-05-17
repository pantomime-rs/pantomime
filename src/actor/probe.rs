use super::*;
use crate::mailbox::CrossbeamSegQueueMailboxLogic;
use std::{thread, time};

/// Provides a means of receiving messages from a non-actor context.
///
/// Internally, this spawns an actor that can receive messages of the
/// specified type, and places them into a buffer that is consumed
/// by the caller.
///
/// When a `Probe` is dropped, its underlying actor is stopped.
pub struct Probe<M: 'static + Send> {
    pub actor_ref: ActorRef<M>,
    mailbox: Mailbox<M>,
    poll_interval: time::Duration,
}

pub trait SpawnProbe {
    fn spawn_probe<M: 'static + Send>(&mut self) -> Probe<M>;
}

impl SpawnProbe for ActiveActorSystem {
    fn spawn_probe<M: 'static + Send>(&mut self) -> Probe<M> {
        let (probe_actor, mailbox) = ProbeActor::new();

        let actor_ref = self.spawn(probe_actor);

        Probe::new(actor_ref, mailbox)
    }
}

impl<N> SpawnProbe for ActorContext<N>
where
    N: 'static + Send,
{
    fn spawn_probe<M: 'static + Send>(&mut self) -> Probe<M> {
        let (probe_actor, mailbox) = ProbeActor::new();

        let actor_ref = self.spawn(probe_actor);

        Probe::new(actor_ref, mailbox)
    }
}

impl<M: 'static + Send> Probe<M> {
    fn new(actor_ref: ActorRef<M>, mailbox: Mailbox<M>) -> Self {
        Self {
            actor_ref,
            mailbox,
            poll_interval: time::Duration::from_millis(10),
        }
    }

    /// Sets the poll interval for this probe. This defines the time that the probe
    /// will sleep when its mailbox is empty.
    pub fn with_poll_interval(mut self, poll_interval: time::Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Receive a message from the mailbox, waiting upto the specified
    /// limit. This is intended for use in test code -- it blocks the
    /// calling thread and polls the mailbox periodically.
    ///
    /// If `limit` elapses, the thread panics.
    pub fn receive(&mut self, limit: time::Duration) -> M {
        let start = time::Instant::now();

        loop {
            let next = self.mailbox.retrieve();

            if next.is_some() {
                return next.unwrap();
            } else if start.elapsed() > limit {
                panic!("provided function hasn't returned true within {:?}", limit);
            } else {
                thread::sleep(self.poll_interval);
            }
        }
    }
}

impl<M: 'static + Send> Drop for Probe<M> {
    fn drop(&mut self) {
        while let Some(m) = self.mailbox.retrieve() {
            drop(m);
        }

        self.actor_ref.stop();
    }
}

struct ProbeActor<M> {
    appender: MailboxAppender<M>,
}

impl<M: 'static + Send> ProbeActor<M> {
    fn new() -> (Self, Mailbox<M>) {
        let mut mailbox = Mailbox::new(CrossbeamSegQueueMailboxLogic::new());

        let probe_actor = Self {
            appender: mailbox.appender(),
        };

        (probe_actor, mailbox)
    }
}

impl<M: 'static + Send> Actor<M> for ProbeActor<M> {
    fn receive(&mut self, message: M, _context: &mut ActorContext<M>) {
        self.appender.append(message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, time};

    struct Doubler;

    enum DoublerMsg {
        Double(usize, ActorRef<usize>),
    }

    impl Actor<DoublerMsg> for Doubler {
        fn receive(&mut self, msg: DoublerMsg, _context: &mut ActorContext<DoublerMsg>) {
            match msg {
                DoublerMsg::Double(number, reply_to) => {
                    reply_to.tell(number * 2);
                }
            }
        }
    }

    #[test]
    fn test_probe() {
        struct TestReaper;

        impl Actor<()> for TestReaper {
            fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

            fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let mut probe = ctx.spawn_probe::<usize>();

                    let doubler = ctx.spawn(Doubler);

                    doubler.tell(DoublerMsg::Double(4, probe.actor_ref.clone()));

                    assert_eq!(probe.receive(time::Duration::from_secs(10)), 8);

                    // @TODO can we improve this test to assert that dropping
                    // the probe stops the actor?

                    ctx.actor_ref().drain();
                }
            }
        }

        assert!(ActorSystem::spawn(TestReaper).is_ok());
    }
}
