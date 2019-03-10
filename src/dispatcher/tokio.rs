use super::*;
use futures::Future;
use std::io;
use tokio_executor::{Executor, SpawnError};
use tokio_reactor::Reactor;
use tokio_timer::timer::{self, Timer};

pub trait RunTokioFuture {
    fn run<F: 'static + Future<Item = (), Error = ()> + Send>(&self, f: F)
        -> Result<(), io::Error>;
}

impl Executor for WorkStealingDispatcher {
    fn spawn(
        &mut self,
        future: Box<Future<Item = (), Error = ()> + Send>,
    ) -> Result<(), SpawnError> {
        self.spawn_future(future);
        // @TODO error
        Ok(())
    }
}

impl RunTokioFuture for WorkStealingDispatcher {
    fn run<F: 'static + Future<Item = (), Error = ()> + Send>(
        &self,
        f: F,
    ) -> Result<(), io::Error> {
        let reactor = Reactor::new()?;
        let reactor_handle = reactor.handle();
        let timer = Timer::new(reactor);
        let timer_handle = timer.handle();

        let mut enter = tokio_executor::enter().expect("pantomime bug: multiple executors at once");

        tokio_reactor::with_default(&reactor_handle, &mut enter, |enter| {
            timer::with_default(&timer_handle, enter, |enter| {
                let mut d1 = self.clone();
                let d2 = self.clone();

                tokio_executor::with_default(&mut d1, enter, |_enter| {
                    d2.spawn_future(f);
                });
            });
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
