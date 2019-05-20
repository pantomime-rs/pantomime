pub mod cancel;
pub mod detached_logic;
pub mod first;
pub mod for_each;
pub mod ignore;
pub mod last;
pub mod tell;

pub use cancel::Cancel;
pub use first::First;
pub use for_each::ForEach;
pub use ignore::Ignore;
pub use last::Last;
pub use tell::Tell;

use crate::actor::ActorRef;
use crate::stream::sink::detached_logic::DetachedLogicSink;
use crate::stream::Stage;
use tell::{TellCommand, TellEvent};

/// Contains methods to construct commonly used `Sink` types.
pub struct Sinks;

impl Sinks {
    pub fn cancel<A, Up: Stage<A>>() -> impl FnOnce(Up) -> Cancel<A, Up>
    where
        A: 'static + Send,
    {
        Cancel::new()
    }

    pub fn first<A, Up: Stage<A>>() -> impl FnOnce(Up) -> First<A, Up>
    where
        A: 'static + Send,
    {
        First::new()
    }

    pub fn for_each<A, F: FnMut(A), Up: Stage<A>>(func: F) -> impl FnOnce(Up) -> ForEach<A, F, Up>
    where
        A: 'static + Send,
        F: 'static + Send,
    {
        ForEach::new(func)
    }

    pub fn ignore<A, Up: Stage<A>>() -> impl FnOnce(Up) -> Ignore<A, Up>
    where
        A: 'static + Send,
    {
        Ignore::new()
    }

    pub fn last<A, Up: Stage<A>>() -> impl FnOnce(Up) -> Last<A, Up>
    where
        A: 'static + Send,
    {
        Last::new()
    }

    pub fn tell<A, Up: Stage<A>>(
        actor_ref: ActorRef<TellEvent<A>>,
    ) -> impl FnOnce(Up) -> DetachedLogicSink<A, TellCommand, Tell<A>, Up>
    where
        A: 'static + Send,
    {
        DetachedLogicSink::new(Tell::new(actor_ref))
    }
}
