use super::*;

enum SingleThreadedDispatcherMessage {
    Execute(Thunk),
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

        Self::spawn(receiver);

        Self { sender }
    }

    fn spawn(receiver: Receiver<SingleThreadedDispatcherMessage>) {
        struct Panicking {
            receiver: Receiver<SingleThreadedDispatcherMessage>,
        }

        impl Drop for Panicking {
            fn drop(&mut self) {
                if thread::panicking() {
                    SingleThreadedDispatcher::spawn(self.receiver.clone());
                }
            }
        }

        thread::spawn(move || {
            let p = Panicking {
                receiver: receiver.clone(),
            };

            loop {
                match receiver.recv() {
                    Ok(SingleThreadedDispatcherMessage::Execute(work)) => {
                        work.apply();
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

impl Dispatcher for SingleThreadedDispatcher {
    fn execute(&self, thunk: Thunk) {
        let _ = self
            .sender
            .send(SingleThreadedDispatcherMessage::Execute(thunk));
    }

    fn safe_clone(&self) -> Box<Dispatcher + Send + Sync> {
        Box::new(Self {
            sender: self.sender.clone(),
        })
    }

    fn shutdown(self) {
        let _ = self.sender.send(SingleThreadedDispatcherMessage::Shutdown);
    }

    fn throughput(&self) -> usize {
        1
    }
}

impl AsRef<Dispatcher + Send + Sync> for SingleThreadedDispatcher {
    fn as_ref(&self) -> &(Dispatcher + Send + Sync + 'static) {
        self
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
