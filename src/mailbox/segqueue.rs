use super::{MailboxAppender, MailboxAppenderLogic, MailboxLogic};
use crossbeam::queue::SegQueue;
use std::sync::Arc;

pub struct CrossbeamSegQueueMailboxAppenderLogic<M> {
    queue: Arc<SegQueue<M>>,
}

impl<M: 'static + Send> MailboxAppenderLogic<M> for CrossbeamSegQueueMailboxAppenderLogic<M> {
    #[inline(always)]
    fn append(&self, message: M) {
        self.queue.push(message);
    }

    fn clone_box(&self) -> Box<dyn MailboxAppenderLogic<M> + Send + Sync> {
        Box::new(Self {
            queue: self.queue.clone(),
        })
    }
}

pub struct CrossbeamSegQueueMailboxLogic<M: 'static + Send> {
    queue: Arc<SegQueue<M>>,
}

impl<M: 'static> CrossbeamSegQueueMailboxLogic<M>
where
    M: Send,
{
    pub fn new() -> Self {
        Self {
            queue: Arc::new(SegQueue::new()),
        }
    }
}

impl<M: 'static + Send> Default for CrossbeamSegQueueMailboxLogic<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: 'static + Send> MailboxLogic<M> for CrossbeamSegQueueMailboxLogic<M> {
    fn appender(&mut self) -> MailboxAppender<M> {
        MailboxAppender::new(CrossbeamSegQueueMailboxAppenderLogic {
            queue: self.queue.clone(),
        })
    }

    fn retrieve(&mut self) -> Option<M> {
        self.queue.pop().ok()
    }
}

#[cfg(test)]
#[allow(clippy::redundant_clone)]
mod tests {
    use super::super::Mailbox;
    use super::*;
    use std::thread;

    #[test]
    fn simple_test() {
        let mut mailbox = Mailbox::new(CrossbeamSegQueueMailboxLogic::new());

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
        let mut mailbox = Mailbox::new(CrossbeamSegQueueMailboxLogic::new());

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
