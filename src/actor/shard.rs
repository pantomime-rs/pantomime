use super::*;
use crate::dispatcher::{Dispatcher, Thunk};
use crate::util::Cancellable;
use crossbeam::atomic::AtomicCell;

enum KernelState {
    Idle(ActorShardKernel),
    Messaged,
    Running
}

pub (in crate::actor) struct ActorShardKernel {
    mailbox: Mailbox<Box<dyn ActorWithMessage + Send + 'static>>,
    system_mailbox: Mailbox<Box<dyn ActorWithSystemMessage + Send + 'static>>,
}

impl ActorShardKernel {
    fn new() -> Self {
        Self {
            mailbox: Mailbox::new(CrossbeamSegQueueMailboxLogic::new()),
            system_mailbox: Mailbox::new(CrossbeamSegQueueMailboxLogic::new()),
        }
    }
}

pub(in crate::actor) struct ActorShard {
    kernel: AtomicCell<KernelState>,
    mailbox_appender: MailboxAppender<Box<ActorWithMessage + Send + 'static>>,
    system_mailbox_appender: MailboxAppender<Box<ActorWithSystemMessage + Send + 'static>>,
    custom_dispatcher: Option<Dispatcher>,
}

impl ActorShard {
    pub(in crate::actor) fn new() -> Self {
        let mut kernel = ActorShardKernel::new();
        let mailbox_appender = kernel.mailbox.appender();
        let system_mailbox_appender = kernel.system_mailbox.appender();

        Self {
            kernel: AtomicCell::new(KernelState::Idle(kernel)),
            mailbox_appender,
            system_mailbox_appender,
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
        match self.kernel.swap(KernelState::Running) {
            KernelState::Idle(kernel) => {
                Some(ActorShardEvent::Scheduled(kernel))
            }

            KernelState::Messaged | KernelState::Running => {
                match self.kernel.swap(KernelState::Messaged) {
                    KernelState::Idle(kernel) => {
                        Some(ActorShardEvent::Scheduled(kernel))
                    }

                    KernelState::Messaged | KernelState::Running => {
                        None
                    }
                }
            }
        }
    }

    pub(in crate::actor) fn scheduled(&self, mut kernel: ActorShardKernel, throughput: usize) -> Option<ActorShardEvent> {
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

        let cont = match self.kernel.swap(KernelState::Idle(kernel)) {
            KernelState::Messaged => {
                true
            }

            KernelState::Running => {
                false
            }

            KernelState::Idle(_) => {
                panic!("pantomime bug: cannot be in Idle");
            }
        } || processed == throughput;

        if cont {
            Some(ActorShardEvent::Messaged)
        } else {
            None
        }
    }
}
