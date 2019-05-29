use super::{MailboxAppender, MailboxAppenderLogic, MailboxLogic};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

/// The appender for `VecDequeMailboxLogic`.
pub struct VecDequeMailboxAppenderLogic<M>
where
    M: 'static + Send,
{
    queue: Arc<Mutex<VecDeque<M>>>,
}

impl<M: 'static + Send> MailboxAppenderLogic<M> for VecDequeMailboxAppenderLogic<M> {
    fn append(&self, message: M) {
        self.queue.lock().push_back(message);
    }

    fn clone_box(&self) -> Box<MailboxAppenderLogic<M> + Send + Sync> {
        Box::new(Self {
            queue: self.queue.clone(),
        })
    }
}

/// A `MailboxLogic` implementation that uses a `VecDeque`
/// behind a `Mutex`.
///
/// This is not particularly performant, but is provided
/// for completeness. Some single threaded systems may
/// find that the performance is fine for their workload.
pub struct VecDequeMailboxLogic<M: 'static + Send> {
    queue: Arc<Mutex<VecDeque<M>>>,
}

impl<M: 'static> VecDequeMailboxLogic<M>
where
    M: Send,
{
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl<M: 'static + Send> Default for VecDequeMailboxLogic<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: 'static + Send> MailboxLogic<M> for VecDequeMailboxLogic<M>
where
    M: 'static + Send,
{
    fn appender(&mut self) -> MailboxAppender<M> {
        MailboxAppender::new(VecDequeMailboxAppenderLogic {
            queue: self.queue.clone(),
        })
    }

    fn retrieve(&mut self) -> Option<M> {
        self.queue.lock().pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::super::Mailbox;
    use super::*;
    use std::thread;

    #[test]
    fn simple_test() {
        let mut mailbox = Mailbox::new(VecDequeMailboxLogic::new());

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
        let mut mailbox = Mailbox::new(VecDequeMailboxLogic::new());

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
