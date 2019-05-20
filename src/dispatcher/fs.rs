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

impl FutureDispatcherBridge for Dispatcher {
    fn spawn_future<F>(&self, future: F)
    where
        F: Future<Item = (), Error = ()> + 'static + Send,
    {
        let dispatcher = self.clone();

        self.execute(move || {
            dispatcher_spawn_continue(dispatcher, Arc::new(Mutex::new(executor::spawn(future))));
        });
    }
}

impl<F> future::Executor<F> for Dispatcher
where
    F: 'static + Future<Item = (), Error = ()> + Send,
{
    fn execute(&self, future: F) -> Result<(), future::ExecuteError<F>> {
        // @TODO error?
        self.spawn_future(future);
        Ok(())
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
fn dispatcher_spawn_continue<F>(dispatcher: Dispatcher, spawn: Arc<Mutex<executor::Spawn<F>>>)
where
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
            let d = dispatcher.clone();

            dispatcher.execute(move || {
                dispatcher_spawn_continue(d, spawn);
            });
        })
    };

    let notifier = Arc::new(Notifier { cont });

    let _ = spawn.lock().poll_future_notify(&notifier, 0);
}
