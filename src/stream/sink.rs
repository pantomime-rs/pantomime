use crate::stream::for_each::ForEach;

pub struct Sink;

impl Sink {
    pub fn for_each<A, F: FnMut(A)>(func: F) -> ForEach<A, F>
    where
        A: 'static + Send,
        F: 'static + Send,
    {
        ForEach::new(func)
    }
}
