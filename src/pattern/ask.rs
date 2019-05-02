use crate::actor::{Actor, ActorContext, ActorRef, ActorSystemContext, Signal};
use futures::*;
use std::time::Duration;

pub trait Ask<Req: 'static + Send, Resp: 'static + Send> {
    /// Send a message to an actor and asynchronously wait for a response. Futures integration.
    ///
    /// The message must contain a field to hold the `ActorRef` that will be sent the reply.
    ///
    /// A future is returned that when executed will form the message to send to the actor,
    /// which will then message the supplied `ActorRef` the response.
    ///
    /// If `timeout` elapses without the actor sending a reply, the returned `Future` will
    /// contain an error of `Canceled`.
    fn ask<F: FnOnce(ActorRef<Resp>) -> Req>(
        &self,
        timeout: Duration,
        f: F,
    ) -> Box<Future<Item = Resp, Error = Canceled> + 'static + Send>
    where
        F: 'static + Send;

    /// Send a message to an actor and asynchronously wait for a response. Futures integration.
    ///
    /// The message must contain a field to hold the `ActorRef` that will be sent the reply.
    ///
    /// A future is returned that when executed will form the message to send to the actor,
    /// which will then message the supplied `ActorRef` the response.
    ///
    /// This wait's (asynchronously) for an indefinite amount of time. This is usually not what
    /// you want, but can be useful for some internal communcation scenarios.
    fn ask_infinite<F: FnOnce(ActorRef<Resp>) -> Req>(
        &self,
        f: F,
    ) -> Box<Future<Item = Resp, Error = ()> + 'static + Send>
    where
        F: 'static + Send;
}

impl<Req: 'static + Send, Resp: 'static + Send> Ask<Req, Resp> for ActorRef<Req> {
    fn ask<F: FnOnce(ActorRef<Resp>) -> Req>(
        &self,
        timeout: Duration,
        f: F,
    ) -> Box<Future<Item = Resp, Error = Canceled> + 'static + Send>
    where
        F: 'static + Send,
    {
        let (c, p) = oneshot::<Resp>();

        let actor_ref = self.system_context().spawn(AskActor::new(c, Some(timeout)));

        let msg = f(actor_ref);

        self.tell(msg);

        Box::new(p)
    }

    fn ask_infinite<F: FnOnce(ActorRef<Resp>) -> Req>(
        &self,
        f: F,
    ) -> Box<Future<Item = Resp, Error = ()> + 'static + Send>
    where
        F: 'static + Send,
    {
        let (c, p) = oneshot::<Resp>();

        let actor_ref = self.system_context().spawn(AskActor::new(c, None));

        let msg = f(actor_ref);

        self.tell(msg);

        Box::new(p.map_err(|_| ()))
    }
}

struct AskActor<Resp: 'static + Send> {
    complete: Option<Complete<Resp>>,
    timeout: Option<Duration>,
}

impl<Resp: 'static + Send> AskActor<Resp> {
    fn new(complete: Complete<Resp>, timeout: Option<Duration>) -> Self {
        Self {
            complete: Some(complete),
            timeout,
        }
    }
}

impl<Resp: 'static + Send> Actor<Resp> for AskActor<Resp> {
    fn receive(&mut self, msg: Resp, context: &mut ActorContext<Resp>) {
        if let Some(complete) = self.complete.take() {
            let _ = complete.send(msg);
        }

        context.actor_ref().stop();
    }

    fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<Resp>) {
        match signal {
            Signal::Started => {
                if let Some(timeout) = self.timeout.take() {
                    let actor_ref = context.actor_ref().clone();

                    context.schedule_thunk(timeout, move || {
                        actor_ref.stop();
                    });
                }
            }

            Signal::Stopped => {
                // this is not technically necessary, but it serves as good doco
                if let Some(complete) = self.complete.take() {
                    drop(complete);
                }
            }

            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use crate::testkit::*;
    use std::{thread, time};

    struct Greeter;

    enum Msg {
        Hello(ActorRef<&'static str>),
        Rude(ActorRef<&'static str>),
    }

    impl Actor<Msg> for Greeter {
        fn receive(&mut self, msg: Msg, _context: &mut ActorContext<Msg>) {
            match msg {
                Msg::Hello(reply_to) => reply_to.tell("Hello!"),

                Msg::Rude(reply_to) => {
                    drop(reply_to);
                }
            }
        }
    }

    #[test]
    fn test_ask_infinite() {
        let mut system = ActorSystem::new().start();

        let greeter = system.spawn(Greeter);

        let reply = greeter
            .ask_infinite(|reply| Msg::Hello(reply))
            .wait()
            .expect("actor didn't reply");

        assert_eq!(reply, "Hello!");
    }

    #[test]
    fn test_ask_no_reply() {
        let mut system = ActorSystem::new().start();

        let greeter = system.spawn(Greeter);

        let is_err = greeter
            .ask(time::Duration::from_millis(100), |reply| Msg::Rude(reply))
            .wait()
            .is_err();

        assert!(is_err);
    }
}
