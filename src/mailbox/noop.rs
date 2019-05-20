use super::{MailboxAppender, MailboxAppenderLogic, MailboxLogic};
use crate::dispatcher::Thunk;
use crate::util::Cancellable;
use std::marker::PhantomData;

/// An appender for `NoopMailbox` that drops any
/// messages sent to it.
pub struct NoopMailboxAppenderLogic<M> {
    data: PhantomData<M>,
}

impl<M> NoopMailboxAppenderLogic<M> {
    pub fn new() -> Self {
        Self { data: PhantomData }
    }
}

impl<M: Send + Sync + 'static> MailboxAppenderLogic<M> for NoopMailboxAppenderLogic<M> {
    fn append(&self, _message: M) {}

    fn append_cancellable(&self, _cancellable: Cancellable, _message: M, _thunk: Option<Thunk>) {}

    fn clone_box(&self) -> Box<MailboxAppenderLogic<M> + Send + Sync> {
        Box::new(NoopMailboxAppenderLogic::new())
    }
}

impl<M: 'static + Send> Default for NoopMailboxAppenderLogic<M> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct NoopMailboxLogic<M> {
    data: PhantomData<M>,
}

impl<M> NoopMailboxLogic<M> {
    pub fn new() -> Self {
        Self { data: PhantomData }
    }
}

impl<M: 'static + Send> Default for NoopMailboxLogic<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Send + Sync + 'static> MailboxLogic<M> for NoopMailboxLogic<M> {
    fn appender(&mut self) -> MailboxAppender<M> {
        MailboxAppender::new(NoopMailboxAppenderLogic::new())
    }

    fn retrieve(&mut self) -> Option<M> {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::mailbox::{Mailbox, NoopMailboxLogic};

    #[test]
    fn simple_test() {
        let mut mailbox = Mailbox::new(NoopMailboxLogic::new());

        assert_eq!(mailbox.retrieve(), None);

        let appender = mailbox.appender();
        appender.append(0);

        let appender2 = appender.clone();
        appender2.append(1);

        assert_eq!(mailbox.retrieve(), None);
    }
}
