//! Mailbox hold messages destined for actors

mod channel;
mod noop;
mod segqueue;

use crate::dispatcher::Thunk;
use crate::util::Cancellable;

pub use self::channel::{CrossbeamChannelMailbox, CrossbeamChannelMailboxAppender};
pub use self::noop::{NoopMailbox, NoopMailboxAppender};
pub use self::segqueue::{CrossbeamSegQueueMailbox, CrossbeamSegQueueMailboxAppender};

/// A `MailboxAppender` is a handle to a mailbox through which messages can be
/// appended.
pub trait MailboxAppender<M> {
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

    fn safe_clone(&self) -> Box<MailboxAppender<M> + Send + Sync>;
}

/// A Mailbox holds messages that are destined for an actor.
///
/// Effectively, this is a multi-producer single-consumer queue.
///
/// To append to a mailbox, call appender to acquire a `MailboxAppender`. The
/// resulting value can be cloned and sent between threads.
///
/// When dropped, calling append on an associated appender should be a noop.
pub trait Mailbox<M> {
    /// Obtain a `MailboxAppender` which can be used to
    /// insert messages into the mailbox.
    fn appender(&mut self) -> Box<MailboxAppender<M> + Send + Sync>;

    /// Retrieve a message from the mailbox, returning
    /// `None` if there are none.
    fn retrieve(&mut self) -> Option<M>;
}
