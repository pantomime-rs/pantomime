use super::{Mailbox, MailboxAppender};
use crate::dispatcher::Thunk;
use crate::util::Cancellable;
use std::marker::PhantomData;

/// An appender for `NoopMailbox` that drops any
/// messages sent to it.
pub struct NoopMailboxAppender<M> {
    data: PhantomData<M>,
}

impl<M> NoopMailboxAppender<M> {
    pub fn new() -> Self {
        Self { data: PhantomData }
    }
}

impl<M: Send + Sync + 'static> MailboxAppender<M> for NoopMailboxAppender<M> {
    fn append(&self, _message: M) {}

    fn append_cancellable(&self, _cancellable: Cancellable, _message: M, _thunk: Option<Thunk>) {}

    fn safe_clone(&self) -> Box<MailboxAppender<M> + Send + Sync> {
        Box::new(NoopMailboxAppender::new())
    }
}

pub struct NoopMailbox<M> {
    data: PhantomData<M>,
}

impl<M> NoopMailbox<M> {
    pub fn new() -> Self {
        Self { data: PhantomData }
    }
}

impl<M: Send + Sync + 'static> Mailbox<M> for NoopMailbox<M> {
    fn appender(&mut self) -> Box<MailboxAppender<M> + Send + Sync> {
        Box::new(NoopMailboxAppender::new())
    }

    fn retrieve(&mut self) -> Option<M> {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::mailbox::{Mailbox, MailboxAppender, NoopMailbox, NoopMailboxAppender};

    #[test]
    fn simple_test() {
        let mut mailbox = NoopMailbox::new();

        assert_eq!(mailbox.retrieve(), None);

        let appender = mailbox.appender();
        appender.append(0);

        let appender2 = appender.safe_clone();
        appender2.append(1);

        assert_eq!(mailbox.retrieve(), None);
    }
}
