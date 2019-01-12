use super::*;
use futures::executor::Notify;
use futures::*;
use futures::{executor, Future};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub trait FutureDispatcherBridge {
    fn spawn_future<F>(&self, future: F)
    where
        F: Future<Item = (), Error = ()> + 'static + Send;
}

impl<D> FutureDispatcherBridge for D
where
    D: AsRef<Dispatcher + Send + Sync>,
{
    fn spawn_future<F>(&self, future: F)
    where
        F: Future<Item = (), Error = ()> + 'static + Send,
    {
        let d_ref = self.as_ref();

        let dispatcher = d_ref.safe_clone();

        d_ref.execute(Box::new(move || {
            dispatcher_spawn_continue(dispatcher, Arc::new(Mutex::new(executor::spawn(future))));
        }));
    }
}

impl<F> future::Executor<F> for WorkStealingDispatcher
where
    F: 'static + Future<Item = (), Error = ()> + Send,
{
    fn execute(&self, future: F) -> Result<(), future::ExecuteError<F>> {
        // @TODO error?
        Ok(self.spawn_future(future))
    }
}

struct Notifier {
    cont: Box<Fn() + Send + 'static + Sync>,
}

impl Notify for Notifier {
    fn notify(&self, _id: usize) {
        (self.cont)();
    }
}

/// Use the provided dispatcher to poll the status often spawn. poll_future_notify
/// is invoked with a notifier that will then recursively make progress until the
/// future is ready.
#[inline(always)]
fn dispatcher_spawn_continue<D, F>(dispatcher: D, spawn: Arc<Mutex<executor::Spawn<F>>>)
where
    D: AsRef<Dispatcher + Send + Sync> + Sync + Send + 'static,
    F: Future<Item = (), Error = ()> + 'static + Send,
{
    let done = AtomicBool::new(false);

    let cont = {
        let spawn = spawn.clone();
        Box::new(move || {
            if done.compare_and_swap(false, true, Ordering::AcqRel) {
                return;
            }
            let spawn = spawn.clone();
            let d_ref = dispatcher.as_ref();
            let d = d_ref.safe_clone();

            d_ref.execute(Box::new(move || {
                dispatcher_spawn_continue(d, spawn);
            }));
        })
    };

    let notifier = Arc::new(Notifier { cont });

    let _ = spawn.lock().poll_future_notify(&notifier, 0);
}
