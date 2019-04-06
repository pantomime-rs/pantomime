use super::*;
use crate::mailbox::{CrossbeamSegQueueMailbox, CrossbeamSegQueueMailboxAppender};
use std::{thread, time};

/// Provides a means of receiving messages from a non-actor context.
///
/// Internally, this spawns an actor that can receive messages of the
/// specified type, and places them into a buffer that is consumed
/// by the caller.
///
/// When a `Probe` is dropped, its underlying actor is automatically
/// stopped.
pub struct Probe<M: 'static + Send> {
    pub actor_ref: ActorRef<M>,
    mailbox: CrossbeamSegQueueMailbox<M>,
}

pub trait SpawnProbe {
    fn spawn_probe<M: 'static + Send>(&mut self) -> Probe<M>;
}

impl SpawnProbe for ActiveActorSystem {
    fn spawn_probe<M: 'static + Send>(&mut self) -> Probe<M> {
        Probe::new(self)
    }
}

impl<M: 'static + Send> Probe<M> {
    fn new(system: &mut ActiveActorSystem) -> Self {
        let (probe_actor, mailbox) = ProbeActor::new();

        let actor_ref = system.spawn(probe_actor);

        Self { actor_ref, mailbox }
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
                thread::sleep(time::Duration::from_millis(30));
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
    appender: CrossbeamSegQueueMailboxAppender<M>,
}

impl<M: 'static + Send> ProbeActor<M> {
    fn new() -> (Self, CrossbeamSegQueueMailbox<M>) {
        let mailbox = CrossbeamSegQueueMailbox::new();
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
        let mut system = ActorSystem::new().start();

        let mut probe = system.spawn_probe::<usize>();

        let doubler = system.spawn(Doubler);

        doubler.tell(DoublerMsg::Double(4, probe.actor_ref.clone()));

        assert_eq!(probe.receive(time::Duration::from_secs(10)), 8);

        drop(probe);

        {
            let context = system.context.clone();

            thread::spawn(move || {
                let c = context.clone();

                context.dispatcher().execute(Box::new(move || {
                    c.drain();
                }));
            });
        }

        system.join();
    }
}
