use crate::stream::for_each::ForEach;
use crate::stream::ignore::Ignore;

/// A `Sink` is a convention for a `Subscriber` that is
/// not a `Publisher`, i.e. it has one input and zero
/// outputs.
pub struct Sink;

impl Sink {
    pub fn for_each<A, F: FnMut(A)>(func: F) -> ForEach<A, F>
    where
        A: 'static + Send,
        F: 'static + Send,
    {
        ForEach::new(func)
    }

    pub fn ignore<A>() -> Ignore<A>
    where
        A: 'static + Send,
    {
        Ignore::new()
    }
}
