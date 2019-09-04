use super::{MailboxAppender, MailboxAppenderLogic, MailboxLogic};
use conqueue::{Queue, QueueReceiver, QueueSender};

pub struct ConqueueMailboxAppenderLogic<M> {
    sender: QueueSender<M>,
}

pub struct ConqueueMailboxLogic<M> {
    sender: QueueSender<M>,
    receiver: QueueReceiver<M>,
}

impl<M> ConqueueMailboxLogic<M>
where
    M: 'static + Send,
{
    /// Creates a new `ConqueueMailbox` which uses
    /// a Conqueue instance to store messages.
    ///
    /// This is the default mailbox and has excellent
    /// general performance characteristics.
    pub fn new() -> Self {
        let (sender, receiver) = Queue::unbounded();

        Self { sender, receiver }
    }
}

impl<M: 'static + Send> Default for ConqueueMailboxLogic<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: 'static + Send> MailboxAppenderLogic<M> for ConqueueMailboxAppenderLogic<M> {
    fn append(&self, message: M) {
        self.sender.push(message);
    }

    fn clone_box(&self) -> Box<dyn MailboxAppenderLogic<M> + Send + Sync> {
        Box::new(ConqueueMailboxAppenderLogic {
            sender: self.sender.clone(),
        })
    }
}

impl<M: 'static + Send> MailboxLogic<M> for ConqueueMailboxLogic<M> {
    fn appender(&mut self) -> MailboxAppender<M> {
        let appender = ConqueueMailboxAppenderLogic {
            sender: self.sender.clone(),
        };

        MailboxAppender::new(appender)
    }

    fn retrieve(&mut self) -> Option<M> {
        self.receiver.pop()
    }
}

#[cfg(test)]
mod tests {
    use crate::mailbox::{ConqueueMailboxLogic, Mailbox};
    use std::thread;

    #[test]
    fn simple_test() {
        let mut mailbox = Mailbox::new(ConqueueMailboxLogic::new());

        assert_eq!(mailbox.retrieve(), None);

        let appender = mailbox.appender();
        appender.append(0);

        let appender2 = appender.clone();
        appender2.append(1);

        assert_eq!(mailbox.retrieve(), Some(0));
        assert_eq!(mailbox.retrieve(), Some(1));
        assert_eq!(mailbox.retrieve(), None);
    }

    #[test]
    fn test_multiple_threads() {
        let mut mailbox = Mailbox::new(ConqueueMailboxLogic::new());

        assert_eq!(mailbox.retrieve(), None);

        let appender = mailbox.appender();
        appender.append(0);

        let mut handles = Vec::new();

        for i in 1..9 {
            let appender = appender.clone();

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
