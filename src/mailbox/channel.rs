use super::{Mailbox, MailboxAppender};
use crate::dispatcher::Thunk;
use crate::util::{Cancellable, MaybeCancelled};
use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};
use std::sync::atomic::Ordering;

pub struct CrossbeamChannelMailboxAppender<M> {
    sender: Sender<MaybeCancelled<M>>,
}

pub struct CrossbeamChannelMailbox<M> {
    sender: Sender<MaybeCancelled<M>>,
    receiver: Receiver<MaybeCancelled<M>>,
}

impl<M> CrossbeamChannelMailbox<M> {
    /// Creates a new `CrossbeamChannelMailbox` which uses
    /// an unbounded Crossbeam channel to store messages.
    ///
    /// This is the default mailbox and has excellent
    /// general performance characteristics.
    pub fn new() -> Self {
        let (sender, receiver) = unbounded();

        CrossbeamChannelMailbox { sender, receiver }
    }

    /// Acquire a non-boxed appender that can append messages
    /// into this mailbox. This is special cased to avoid an
    /// additional level of indirection that the regular
    /// appender implies.
    ///
    /// This exists given the performance critical nature of
    /// the `CrossbeamChannelMailbox`, which is the default
    /// implementation and also used for all system messages.
    pub fn appender(&self) -> CrossbeamChannelMailboxAppender<M> {
        CrossbeamChannelMailboxAppender {
            sender: self.sender.clone(),
        }
    }
}

impl<M: 'static + Send> MailboxAppender<M> for CrossbeamChannelMailboxAppender<M> {
    fn append(&self, message: M) {
        if let Err(_e) = self.sender.send(MaybeCancelled::new(message, None, None)) {
            // @TODO this is normal -- appender still exists, but receiver dropped
        }
    }

    fn append_cancellable(&self, cancellable: Cancellable, message: M, thunk: Option<Thunk>) {
        if let Err(_e) = self.sender.send(MaybeCancelled::new(
            message,
            Some(cancellable.cancel),
            thunk,
        )) {
            // @TODO this is normal -- appender still exists, but receiver dropped
        }
    }

    fn safe_clone(&self) -> Box<MailboxAppender<M> + Send + Sync> {
        Box::new(CrossbeamChannelMailboxAppender {
            sender: self.sender.clone(),
        })
    }
}

impl<M: 'static + Send> Mailbox<M> for CrossbeamChannelMailbox<M> {
    fn appender(&mut self) -> Box<MailboxAppender<M> + Send + Sync> {
        let appender = CrossbeamChannelMailboxAppender {
            sender: self.sender.clone(),
        };

        Box::new(appender)
    }

    fn retrieve(&mut self) -> Option<M> {
        match self.receiver.try_recv() {
            Ok(msg) => match msg.cancelled {
                None => {
                    if let Some(thunk) = msg.handled {
                        thunk.apply();
                    }

                    Some(msg.item)
                }

                Some(ref c) if c.load(Ordering::Acquire) => None,

                Some(_) => {
                    if let Some(thunk) = msg.handled {
                        thunk.apply();
                    }

                    Some(msg.item)
                }
            },

            Err(TryRecvError::Empty) => None,

            Err(TryRecvError::Disconnected) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::mailbox::{CrossbeamChannelMailbox, Mailbox, MailboxAppender};
    use std::thread;

    #[test]
    fn simple_test() {
        let mut mailbox = CrossbeamChannelMailbox::new();

        assert_eq!(mailbox.retrieve(), None);

        let appender = mailbox.appender();
        appender.append(0);

        let appender2 = appender.safe_clone();
        appender2.append(1);

        assert_eq!(mailbox.retrieve(), Some(0));
        assert_eq!(mailbox.retrieve(), Some(1));
        assert_eq!(mailbox.retrieve(), None);
    }

    #[test]
    fn test_multiple_threads() {
        let mut mailbox = CrossbeamChannelMailbox::new();

        assert_eq!(mailbox.retrieve(), None);

        let appender = mailbox.appender();
        appender.append(0);

        let mut handles = Vec::new();

        for i in 1..9 {
            let appender = appender.safe_clone();

            handles.push(thread::spawn(move || {
                for j in (i * 100)..(i * 100) + 50 {
                    appender.append(j);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // there isn't a per-thread order guarantee, but
        // for each same hundredths place, they should be
        // in order

        let mut messages = Vec::new();

        while let Some(message) = mailbox.retrieve() {
            messages.push(message);
        }

        for (i, m) in messages.iter().enumerate() {
            for (j, n) in messages.iter().enumerate() {
                if (m / 100) == (n / 100) && j > i {
                    assert!(m < n);
                }
            }
        }
    }
}
