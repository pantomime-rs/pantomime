use crate::actor::ActorRef;
use futures::Future;

#[cfg(feature = "futures-support")]
pub trait PipeTo<M: 'static + Send, E> {
    /// Create a future that upon completion pipes the result to the provided actor.
    ///
    /// This does not spawn the future -- use context.spawn_future to do that.
    #[must_use]
    fn pipe_to(self, actor_ref: ActorRef<M>) -> Box<Future<Item = (), Error = E> + Send>;
}

#[cfg(feature = "futures-support")]
impl<M: 'static + Send, E, F> PipeTo<M, E> for F
where
    F: 'static + Future<Item = M, Error = E> + Send,
{
    #[must_use]
    fn pipe_to(self, actor_ref: ActorRef<M>) -> Box<Future<Item = (), Error = E> + Send> {
        let actor_ref = actor_ref.clone();

        let f = self.map(move |r| actor_ref.tell(r));

        Box::new(f)
    }
}
