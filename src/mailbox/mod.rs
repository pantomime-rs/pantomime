//! Mailbox hold messages destined for actors

mod channel;
mod noop;
mod segqueue;
mod vecdeque;

use crate::dispatcher::Thunk;
use crate::util::Cancellable;

pub use self::channel::CrossbeamChannelMailboxLogic;
pub use self::noop::NoopMailboxLogic;
pub use self::segqueue::CrossbeamSegQueueMailboxLogic;
pub use self::vecdeque::VecDequeMailboxLogic;

pub struct MailboxAppender<M> {
    logic: Box<MailboxAppenderLogic<M> + 'static + Send + Sync>,
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

    pub fn append_cancellable(&self, cancellable: Cancellable, message: M, thunk: Option<Thunk>) {
        self.logic.append_cancellable(cancellable, message, thunk);
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

    /// Appends a special kind of cancellable message to the mailbox.
    ///
    /// Implementations must check the cancelled flag of the returned
    /// `Cancellable`, and if it is true, drop the message instead of
    /// returning it to the caller. If it is false, the provided thunk
    /// must be called, and the message should be returned to the caller.
    ///
    /// In general, this is not what you want.
    fn append_cancellable(&self, cancellable: Cancellable, message: M, thunk: Option<Thunk>);

    fn clone_box(&self) -> Box<MailboxAppenderLogic<M> + Send + Sync>;
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
    logic: Box<MailboxLogic<M> + 'static + Send + Sync>,
}

impl<M> Mailbox<M> {
    pub fn new<Logic: MailboxLogic<M>>(logic: Logic) -> Self
    where
        Logic: 'static + Send + Sync,
    {
        Self {
            logic: Box::new(logic),
        }
    }

    pub(in crate) fn appender(&mut self) -> MailboxAppender<M> {
        self.logic.appender()
    }

    pub(in crate) fn retrieve(&mut self) -> Option<M> {
        self.logic.retrieve()
    }
}
