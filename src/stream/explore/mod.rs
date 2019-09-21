
struct SourceQueue<A> {
    phantom: PhantomData<A>
}

impl<A> SourceQueue<A> {
    fn new() -> Self {
        Self {
            phantom: PhantomData
        }
    }
}

struct MergeHub<A> {
    phantom: PhantomData<A>,
}

impl<A> MergeHub<A> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<A> Actor<Source<A>> for MergeHub<A>
    where
        A: 'static + Send,
{
    fn receive(&mut self, source: Source<A>, _: &mut ActorContext<Source<A>>) {}
}

struct BroadcastHub<A> {
    phantom: PhantomData<A>,
}

impl<A> BroadcastHub<A> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<A> Actor<Sink<A, ()>> for BroadcastHub<A>
    where
        A: 'static + Send + Clone,
{
    fn receive(&mut self, source: Sink<A, ()>, _: &mut ActorContext<Sink<A, ()>>) {}
}

