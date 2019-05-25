use super::*;
use crossbeam::atomic::AtomicCell;
use futures::executor::Notify;
use futures::*;
use futures::{executor, Async, Future};
use std::sync::Arc;

pub trait FutureDispatcherBridge {
    fn spawn_future<F>(&self, future: F)
    where
        F: Future<Item = (), Error = ()> + 'static + Send;
}

impl FutureDispatcherBridge for Dispatcher {
    fn spawn_future<F>(&self, future: F)
    where
        F: Future<Item = (), Error = ()> + 'static + Send,
    {
        self.execute(move || {
            let notifier = Arc::new(Notifier::<F> {
                state: AtomicCell::new(NotifierState::NotNotified),
            });

            notifier.cont(executor::spawn(future));
        });
    }
}

impl<F> future::Executor<F> for Dispatcher
where
    F: 'static + Future<Item = (), Error = ()> + Send,
{
    fn execute(&self, future: F) -> Result<(), future::ExecuteError<F>> {
        self.spawn_future(future);

        Ok(())
    }
}

enum NotifierState<F: Future<Item = (), Error = ()>>
where
    F: 'static + Send,
{
    Waiting(Arc<Notifier<F>>, executor::Spawn<F>),
    Notified,
    NotNotified,
}

struct Notifier<F: Future<Item = (), Error = ()>>
where
    F: 'static + Send,
{
    state: AtomicCell<NotifierState<F>>,
}

impl<F: Future<Item = (), Error = ()>> Notifier<F>
where
    F: 'static + Send,
{
    fn cont(self: Arc<Self>, mut spawn: executor::Spawn<F>) {
        match spawn.poll_future_notify(&self, 0) {
            Ok(Async::NotReady) => {
                self.provide(spawn);
            }

            Ok(Async::Ready(())) => {}

            Err(()) => {}
        }
    }

    fn provide(self: Arc<Self>, spawn: executor::Spawn<F>) {
        match self.state.swap(NotifierState::Waiting(self.clone(), spawn)) {
            NotifierState::Notified => {
                if let NotifierState::Waiting(_, spawn) =
                    self.state.swap(NotifierState::NotNotified)
                {
                    self.cont(spawn);
                }
            }

            NotifierState::Waiting(_, _) => {
                panic!("pantomime bug: cannot be in Waiting state");
            }

            NotifierState::NotNotified => {}
        }
    }
}

impl<F: Future<Item = (), Error = ()>> Notify for Notifier<F>
where
    F: 'static + Send,
{
    fn notify(&self, _: usize) {
        match self.state.swap(NotifierState::Notified) {
            NotifierState::Waiting(this, spawn) => {
                this.cont(spawn);
            }

            NotifierState::Notified => {}

            NotifierState::NotNotified => {}
        }
    }
}
