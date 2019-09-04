//! Mailbox hold messages destined for actors

mod channel;
mod conqueue;
mod noop;
mod segqueue;
mod vecdeque;

pub use self::channel::CrossbeamChannelMailboxLogic;
pub use self::conqueue::ConqueueMailboxLogic;
pub use self::noop::NoopMailboxLogic;
pub use self::segqueue::CrossbeamSegQueueMailboxLogic;
pub use self::vecdeque::VecDequeMailboxLogic;

pub struct MailboxAppender<M> {
    logic: Box<dyn MailboxAppenderLogic<M> + 'static + Send + Sync>,
}

impl<M> MailboxAppender<M> {
    pub fn new<Logic: MailboxAppenderLogic<M>>(logic: Logic) -> Self
    where
        Logic: 'static + Send + Sync,
    {
        Self {
            logic: Box::new(logic),
        }
    }

    pub fn append(&self, message: M) {
        self.logic.append(message);
    }
}

impl<M> Clone for MailboxAppender<M> {
    fn clone(&self) -> Self {
        Self {
            logic: self.logic.clone_box(),
        }
    }
}

/// A `MailboxAppender` is a handle to a mailbox through which messages can be
/// appended.
pub trait MailboxAppenderLogic<M> {
    /// Append the supplied message to the mailbox.
    ///
    /// Calls to `Mailbox::retrieve` should then eventually return the
    /// message, as implementers see fit.
    fn append(&self, message: M);

    fn clone_box(&self) -> Box<dyn MailboxAppenderLogic<M> + Send + Sync>;
}

/// A Mailbox holds messages that are destined for an actor.
///
/// Effectively, this is a multi-producer single-consumer queue.
///
/// To append to a mailbox, call appender to acquire a `MailboxAppender`. The
/// resulting value can be cloned and sent between threads.
///
/// When dropped, calling append on an associated appender should be a noop.
pub trait MailboxLogic<M> {
    /// Obtain a `MailboxAppender` which can be used to
    /// insert messages into the mailbox.
    fn appender(&mut self) -> MailboxAppender<M>;

    /// Retrieve a message from the mailbox, returning
    /// `None` if there are none.
    fn retrieve(&mut self) -> Option<M>;
}

pub struct Mailbox<M> {
    logic: Box<dyn MailboxLogic<M> + 'static + Send>,
}

impl<M> Mailbox<M> {
    pub fn new<Logic: MailboxLogic<M>>(logic: Logic) -> Self
    where
        Logic: 'static + Send,
    {
        Self {
            logic: Box::new(logic),
        }
    }

    pub fn new_boxed(logic: Box<dyn MailboxLogic<M> + Send>) -> Self {
        Self { logic }
    }

    pub(in crate) fn appender(&mut self) -> MailboxAppender<M> {
        self.logic.appender()
    }

    pub(in crate) fn retrieve(&mut self) -> Option<M> {
        self.logic.retrieve()
    }
}
