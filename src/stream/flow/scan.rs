use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use std::marker::PhantomData;

pub struct Scan<A, B, F: FnMut(B, A) -> B>
where
    A: Send,
    B: Clone + Send,
{
    scan_fn: F,
    phantom: PhantomData<(A, B)>,
    sent: bool,
    last: Option<B>,
}

impl<A, B, F: FnMut(B, A) -> B> Scan<A, B, F>
where
    A: Send,
    B: Clone + Send,
{
    pub fn new(zero: B, scan_fn: F) -> Self {
        Self {
            scan_fn,
            phantom: PhantomData,
            sent: false,
            last: Some(zero),
        }
    }
}

impl<A, B, F: FnMut(B, A) -> B> Logic for Scan<A, B, F>
where
    A: 'static + Send,
    B: 'static + Clone + Send,
    F: 'static + Send,
{
    type In = A;
    type Out = B;
    type Msg = ();

    fn receive(
        &mut self,
        msg: LogicEvent<Self::In, Self::Msg>,
        ctx: &mut StreamContext<Self::In, Self::Out, Self::Msg, Self>,
    ) {
        match msg {
            LogicEvent::Pushed(element) => {
                let last = self.last.take().expect("pantomime bug: Scan::last is None");
                let next = (self.scan_fn)(last, element);

                ctx.tell(Action::Push(next.clone()));

                self.last = Some(next);
            }

            LogicEvent::Pulled if self.sent => {
                ctx.tell(Action::Pull);
            }

            LogicEvent::Pulled => {
                self.sent = true;

                let last = self
                    .last
                    .as_ref()
                    .expect("pantomime bug: Scan::last is None")
                    .clone();

                ctx.tell(Action::Push(last))
            }

            LogicEvent::Stopped | LogicEvent::Cancelled => {
                ctx.tell(Action::Complete(None));
            }

            LogicEvent::Started | LogicEvent::Forwarded(()) => {}
        }
    }
}
