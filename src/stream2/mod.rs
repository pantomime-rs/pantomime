trait Stage<A>: Producer<A> {

}

trait Producer<A> {

}

pub struct Error;

pub struct StreamContext;

pub enum AsyncAction<B, Msg> {
    Cancel,
    Complete,
    Fail(Error),
    Forward(Msg),
    Pull,
    Push(B),
}

trait AsyncFlowLogic<A, B, Msg> {
    #[must_use]
    fn attach(
        &mut self,
        upstream: Box<Stage<A>>,
        context: &StreamContext,
    ) -> Option<AsyncAction<B, Msg>>;

    #[must_use]
    fn forwarded(&mut self, msg: Msg) -> Option<AsyncAction<B, Msg>>;

    #[must_use]
    fn produced(&mut self, elem: A) -> Option<AsyncAction<B, Msg>>;

    #[must_use]
    fn pulled(&mut self) -> Option<AsyncAction<B, Msg>>;

    #[must_use]
    fn completed(&mut self) -> Option<AsyncAction<B, Msg>>;

    #[must_use]
    fn failed(&mut self, error: Error) -> Option<AsyncAction<B, Msg>>;
}

trait AsyncSourceLogic<A> {

}

trait AsyncSinkLogic<A> {

}

enum FlowLogic<A, B, Msg> {
    Async(Box<AsyncFlowLogic<A, B, Msg>>)
}

struct Flow<A, B> {
    logic: Box<(A, B)>
}

pub enum AsyncSourceAction<A, Msg> {
    Complete,
    Fail(Error),
    Forward(Msg),
    Push(A),
}

enum SourceLogic<A, Msg> {
    Async(Box<AsyncSourceAction<A, Msg>>)
}

struct Source<A> {
    producer: Box<Producer<A>>,
}

struct Sink<A> {
    wort: A
}

impl<A> Source<A> {
    fn via<B>(self, flow: Flow<A, B>) -> Source<B> {
        panic!()
    }

    fn to(self, sink: Sink<A>) -> SpawnableStream {
        SpawnableStream
    }
}

struct SpawnableStream;



















