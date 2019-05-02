use super::*;
use crate::dispatcher::{Dispatcher, Thunk};
use crate::util::Cancellable;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct ActorShardKernel {
    mailbox: CrossbeamSegQueueMailbox<Box<dyn ActorWithMessage + Send + 'static>>,
    running: Arc<AtomicBool>,
    system_mailbox: CrossbeamSegQueueMailbox<Box<dyn ActorWithSystemMessage + Send + 'static>>,
}

impl ActorShardKernel {
    fn new() -> Self {
        Self {
            mailbox: CrossbeamSegQueueMailbox::new(),
            running: Arc::new(AtomicBool::new(false)),
            system_mailbox: CrossbeamSegQueueMailbox::new(),
        }
    }
}

pub(in crate::actor) struct ActorShard {
    kernel: Mutex<ActorShardKernel>,
    mailbox_appender: CrossbeamSegQueueMailboxAppender<Box<dyn ActorWithMessage + Send + 'static>>,
    system_mailbox_appender:
        CrossbeamSegQueueMailboxAppender<Box<ActorWithSystemMessage + Send + 'static>>,
    running: Arc<AtomicBool>,
    custom_dispatcher: Option<Dispatcher>,
}

impl ActorShard {
    pub(in crate::actor) fn new() -> Self {
        let kernel = ActorShardKernel::new();
        let mailbox_appender = kernel.mailbox.appender();
        let system_mailbox_appender = kernel.system_mailbox.appender();
        let running = kernel.running.clone();
        let kernel = Mutex::new(kernel);

        Self {
            kernel,
            mailbox_appender,
            system_mailbox_appender,
            running,
            custom_dispatcher: None,
        }
    }

    pub(in crate::actor) fn with_dispatcher(mut self, dispatcher: Dispatcher) -> Self {
        self.custom_dispatcher = Some(dispatcher);
        self
    }

    pub(in crate::actor) fn custom_dispatcher(&self) -> Option<Dispatcher> {
        self.custom_dispatcher.as_ref().map(|d| d.clone())
    }

    #[inline(always)]
    pub(in crate::actor) fn tell(&self, msg: Box<dyn ActorWithMessage + Send + 'static>) {
        self.mailbox_appender.append(msg);
    }

    pub(in crate::actor) fn tell_cancellable(
        &self,
        cancellable: Cancellable,
        msg: Box<dyn ActorWithMessage + Send + 'static>,
        thunk: Option<Thunk>,
    ) {
        self.mailbox_appender
            .append_cancellable(cancellable, msg, thunk)
    }

    pub(in crate::actor) fn tell_system(&self, msg: Box<ActorWithSystemMessage + Send + 'static>) {
        self.system_mailbox_appender.append(msg);
    }

    pub(in crate::actor) fn messaged(&self) -> Option<ActorShardEvent> {
        if !self
            .running
            .compare_and_swap(false, true, Ordering::Acquire)
        {
            Some(ActorShardEvent::Scheduled)
        } else {
            None
        }
    }

    pub(in crate::actor) fn scheduled(&self, throughput: usize) -> Option<ActorShardEvent> {
        // note that in the typical case, acquiring the lock will not block
        // as we're guarded behind a ready CAS. There can be rare conditions
        // where it will though.
        let mut kernel = self.kernel.lock();

        self.running.store(false, Ordering::Release);

        let mut processed = 0;

        while let Some(system_msg) = kernel.system_mailbox.retrieve() {
            system_msg.apply();

            processed += 1;

            if processed == throughput {
                break;
            }
        }

        if processed < throughput {
            while let Some(message) = kernel.mailbox.retrieve() {
                message.apply();

                processed += 1;

                if processed == throughput {
                    break;
                }

                while let Some(system_msg) = kernel.system_mailbox.retrieve() {
                    system_msg.apply();

                    processed += 1;

                    if processed == throughput {
                        break;
                    }
                }
            }
        }

        let cont = processed == throughput;

        if self
            .running
            .compare_and_swap(true, false, Ordering::Release)
            || cont
        {
            // we've been notified that it's time to execute again. even if
            // we've stopped, we schedule for execution again. we'll change
            // status on the next go, and then notify the watcher
            Some(ActorShardEvent::Messaged)
        } else {
            None
        }
    }
}
