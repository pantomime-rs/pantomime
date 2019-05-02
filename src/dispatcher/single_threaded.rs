use super::*;

enum SingleThreadedDispatcherMessage {
    Execute(Thunk),
    ExecuteTrampoline(Trampoline),
    Shutdown,
}

/// A single threaded dispatcher that is backed by crossbeam_channel.
///
/// This is an MPSC queue that allows producer handlers (senders) to
/// be cloned. The worker thread owns the consumer handler (receiver)
/// and executes jobs.
///
/// This is used for actors that are configured to use a dedicated
/// thread for their execution. In particular, the timer actor uses
/// it.
pub struct SingleThreadedDispatcher {
    sender: Sender<SingleThreadedDispatcherMessage>,
}

impl SingleThreadedDispatcher {
    pub fn new() -> Self {
        let (sender, receiver) = unbounded::<SingleThreadedDispatcherMessage>();

        Self::spawn(sender.clone(), receiver);

        Self { sender }
    }

    fn spawn(
        sender: Sender<SingleThreadedDispatcherMessage>,
        receiver: Receiver<SingleThreadedDispatcherMessage>,
    ) {
        struct Panicking {
            receiver: Receiver<SingleThreadedDispatcherMessage>,
            sender: Sender<SingleThreadedDispatcherMessage>,
        }

        impl Drop for Panicking {
            fn drop(&mut self) {
                if thread::panicking() {
                    SingleThreadedDispatcher::spawn(self.sender.clone(), self.receiver.clone());
                }
            }
        }

        thread::spawn(move || {
            let p = Panicking {
                receiver: receiver.clone(),
                sender: sender.clone(),
            };

            loop {
                match receiver.recv() {
                    Ok(SingleThreadedDispatcherMessage::Execute(work)) => {
                        work.apply();
                    }

                    Ok(SingleThreadedDispatcherMessage::ExecuteTrampoline(trampoline)) => {
                        // @TODO this can be improved a bit, probably should run serially a few times

                        match trampoline.step {
                            TrampolineStep::Bounce(produce) => {
                                sender.send(SingleThreadedDispatcherMessage::ExecuteTrampoline(
                                    produce.apply(),
                                )).expect("pantomime bug: SingleThreadedDispatcher sender missing, this shouldn't be possible");
                            }

                            TrampolineStep::Done => (),
                        }
                    }

                    Ok(SingleThreadedDispatcherMessage::Shutdown) => {
                        break;
                    }

                    Err(RecvError) => {
                        // @TODO log this?
                        break;
                    }
                }
            }

            drop(receiver);
            drop(p);
        });
    }
}

impl DispatcherLogic for SingleThreadedDispatcher {
    fn clone_box(&self) -> Box<DispatcherLogic + 'static + Send + Sync> {
        Box::new(Self {
            sender: self.sender.clone(),
        })
    }

    fn execute(&self, thunk: Thunk) {
        let _ = self
            .sender
            .send(SingleThreadedDispatcherMessage::Execute(thunk));
    }

    fn execute_trampoline(&self, trampoline: Trampoline) {
        let _ = self
            .sender
            .send(SingleThreadedDispatcherMessage::ExecuteTrampoline(
                trampoline,
            ));
    }

    fn shutdown(self: Box<Self>) {
        let _ = self.sender.send(SingleThreadedDispatcherMessage::Shutdown);
    }

    fn throughput(&self) -> usize {
        1
    }
}

impl Clone for SingleThreadedDispatcher {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn simple_test_fifo() {
        let counter = Arc::new(AtomicUsize::new(0));

        let dispatcher = SingleThreadedDispatcher::new();

        for _ in 0..100 {
            let counter = counter.clone();

            dispatcher.execute(Box::new(move || {
                counter.fetch_add(10, Ordering::SeqCst);
            }));
        }

        eventually(Duration::from_millis(3000), move || {
            counter.load(Ordering::SeqCst) == 1000
        });
    }

    #[test]
    fn test_panic() {
        let counter = Arc::new(AtomicUsize::new(0));

        let dispatcher = SingleThreadedDispatcher::new();

        dispatcher.execute(Box::new(move || {
            panic!("testing");
        }));

        {
            let counter = counter.clone();

            dispatcher.execute(Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        }

        eventually(Duration::from_millis(10000), move || {
            counter.load(Ordering::SeqCst) == 1
        });
    }
}
