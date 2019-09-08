use crate::actor::*;
use crate::stream::detached::*;
use crate::stream::*;
use mio::{net::UdpSocket, Poll, PollOpt, Ready, Token};
use std::net::SocketAddr;
use std::sync::Arc;
use std::usize;
use crate::dispatcher::Trampoline;
use crate::stream::oxidized::{Consumer, Producer};
use crate::stream::flow::detached::{Detached, AsyncAction, DetachedLogic};
use crate::stream::sink::Sinks;

#[derive(Debug)]
pub struct Datagram {
    pub data: Vec<u8>,
    pub address: SocketAddr,
}

struct UdpSource {
    actor_ref: ActorRef<AsyncAction<Datagram, UdpSourceMsg>>,
    system_context: Option<ActorSystemContext>,
    buffer: Vec<u8>,
    ready: bool,
    socket: UdpSocket,
    waiting: bool,
    poll: Option<Arc<Poll>>,
    token: usize,
}

enum UdpSourceMsg {
    SubscriptionEvent(SubscriptionEvent),
}

impl UdpSource {
    fn try_read(&mut self) -> Option<AsyncAction<Datagram, UdpSourceMsg>> {
        if self.ready && self.waiting {
            match self.socket.recv_from(&mut self.buffer) {
                Ok((bytes_read, socket_addr)) => {
                    self.waiting = false;
                    // @TODO verify buffer logic, perf

                    Some(AsyncAction::Push(Datagram {
                        data: self.buffer[0..bytes_read].to_vec(),
                        address: socket_addr,
                    }))
                }

                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    self.ready = false;
                    None
                }

                Err(_e) => {
                    // @TODO error
                    Some(AsyncAction::Fail(Error))
                }
            }
        } else {
            None
        }
    }
}

// @TODO cancel should be exposed to logic
impl DetachedLogic<(), Datagram, UdpSourceMsg> for UdpSource {
    fn attach(
        &mut self,
        ctx: &StreamContext,
        actor_ref: &ActorRef<AsyncAction<Datagram, UdpSourceMsg>>,
    ) -> Option<AsyncAction<Datagram, UdpSourceMsg>> {
        ctx.system_context().subscribe(
            actor_ref.convert(|s| AsyncAction::Forward(UdpSourceMsg::SubscriptionEvent(s))),
        );

        self.actor_ref = actor_ref.clone();
        self.system_context = Some(ctx.system_context.clone());

        None
    }

    fn forwarded(&mut self, msg: UdpSourceMsg) -> Option<AsyncAction<Datagram, UdpSourceMsg>> {
        match msg {
            UdpSourceMsg::SubscriptionEvent(SubscriptionEvent::MioEvent(event)) => {
                if event.readiness().is_readable() {
                    self.ready = true;
                }
            }

            UdpSourceMsg::SubscriptionEvent(SubscriptionEvent::Ready(poll, token)) => {
                poll.register(
                    &self.socket,
                    Token(token),
                    Ready::readable(),
                    PollOpt::edge(),
                )
                .expect("pantomime bug: failed to register socket");

                self.poll = Some(poll);
                self.token = token;
            }
        }

        self.try_read()
    }

    fn produced(&mut self, _: ()) -> Option<AsyncAction<Datagram, UdpSourceMsg>> {
        None
    }

    fn pulled(&mut self) -> Option<AsyncAction<Datagram, UdpSourceMsg>> {
        self.waiting = true;

        self.try_read()
    }

    fn completed(&mut self) -> Option<AsyncAction<Datagram, UdpSourceMsg>> {
        if let Some(poll) = self.poll.take() {
            if poll.deregister(&self.socket).is_err() {
                error!("failed to deregister UDP socket interest");
            }
        }

        if let Some(ref system_context) = self.system_context {
            system_context.unsubscribe(self.token);
        }

        None
    }

    fn failed(&mut self, _: Error) -> Option<AsyncAction<Datagram, UdpSourceMsg>> {
        if let Some(poll) = self.poll.take() {
            poll.deregister(&self.socket)
                .expect("pantomime bug: failed to deregister socket");
        }

        None
    }
}

enum UdpSinkMsg {
    SubscriptionEvent(SubscriptionEvent),
}

struct Chunk {
    data: Datagram,
    written: usize,
}

struct UdpSink {
    actor_ref: ActorRef<AsyncAction<(), UdpSinkMsg>>,
    chunk: Option<Chunk>,
    ready: bool,
    socket: UdpSocket,
    poll: Option<Arc<Poll>>,
    token: usize,
}

impl UdpSink {
    fn try_write(&mut self) -> Option<AsyncAction<(), UdpSinkMsg>> {
        if self.ready && self.chunk.is_some() {
            if let Some(ref mut chunk) = self.chunk {
                match self
                    .socket
                    .send_to(&chunk.data.data[chunk.written..], &chunk.data.address)
                {
                    Ok(bytes_written) => {
                        chunk.written += bytes_written;

                        if chunk.written == chunk.data.data.len() {
                            self.chunk = None;

                            Some(AsyncAction::Pull)
                        } else {
                            None
                        }
                    }

                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // @TODO error
                        self.ready = false;
                        None
                    }

                    Err(_e) => {
                        // @TODO error
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl Consumer<Datagram> for UdpSink {
    fn started<Produce: Producer<Datagram>>(
        self,
        producer: Produce,
        ctx: &StreamContext,
    ) -> Trampoline {


        Trampoline::done()
    }

    fn produced<Produce: Producer<Datagram>>(self, producer: Produce, element: Datagram) -> Trampoline {
        Trampoline::done()
    }

    fn completed(self) -> Trampoline  {
        Trampoline::done()
    }

    fn failed(self, error: Error) -> Trampoline  {
        Trampoline::done()
    }
}

impl Sink<Datagram> for UdpSink {
    fn start(self, stream_context: &StreamContext) -> Trampoline {
        Trampoline::done()
    }
}

impl DetachedLogic<Datagram, (), UdpSinkMsg> for UdpSink {
    fn attach(
        &mut self,
        ctx: &StreamContext,
        actor_ref: &ActorRef<AsyncAction<(), UdpSinkMsg>>,
    ) -> Option<AsyncAction<(), UdpSinkMsg>> {
        ctx.system_context().subscribe(
            actor_ref.convert(|s| AsyncAction::Forward(UdpSinkMsg::SubscriptionEvent(s))),
        );

        self.actor_ref = actor_ref.clone();

        None
    }

    fn forwarded(&mut self, msg: UdpSinkMsg) -> Option<AsyncAction<(), UdpSinkMsg>> {
        match msg {
            UdpSinkMsg::SubscriptionEvent(SubscriptionEvent::MioEvent(event)) => {
                if event.readiness().is_writable() {
                    self.ready = true;
                }
            }

            UdpSinkMsg::SubscriptionEvent(SubscriptionEvent::Ready(poll, token)) => {
                poll.register(
                    &self.socket,
                    Token(token),
                    Ready::writable(),
                    PollOpt::edge(),
                )
                .unwrap();

                self.poll = Some(poll);
                self.token = token;
            }
        }

        self.try_write()
    }

    fn produced(&mut self, data: Datagram) -> Option<AsyncAction<(), UdpSinkMsg>> {
        if self.chunk.is_some() {
            panic!();
        }

        self.chunk = Some(Chunk { data, written: 0 });

        self.try_write()
    }

    fn pulled(&mut self) -> Option<AsyncAction<(), UdpSinkMsg>> {
        Some(AsyncAction::Pull)
    }

    fn completed(&mut self) -> Option<AsyncAction<(), UdpSinkMsg>> {
        // @TODO close this half of the connection

        Some(AsyncAction::Complete)
    }

    fn failed(&mut self, error: Error) -> Option<AsyncAction<(), UdpSinkMsg>> {
        // @TODO close this half of the connection

        Some(AsyncAction::Fail(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::*;
    use crate::testkit::*;
    use std::thread;
    use std::time;
    use crate::stream::source::Sources;
    use crate::stream::flow::detached::Detached;

    #[test]
    fn test_basic_udp() {
        struct MyTestActor;

        impl Actor<()> for MyTestActor {
            fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<()>) {
                if let Signal::Started = signal {
                    let system_context = context.system_context().clone(); // @TODO don't use system context


                    let udp_source = UdpSource {
                        actor_ref: ActorRef::empty(),
                        system_context: None,
                        buffer: vec![0; 8192],
                        ready: false,
                        socket: UdpSocket::bind(&"127.0.0.1:4521".parse().unwrap()).unwrap(),
                        waiting: false,
                        poll: None,
                        token: 0,
                    };

                  //  let s = Sources::empty().via(Detached::new(udp_source));



                    let sink_socket = udp_source.socket.try_clone().unwrap();

                    context.spawn_stream(
                        Sources::empty()
                            .via(Detached::new(udp_source))
                            .map(|bs: Datagram| {
                                let mut reverse: Vec<u8> = bs
                                    .data
                                    .iter()
                                    .rev()
                                    .map(|b| *b)
                                    .skip_while(|b| *b == 10)
                                    .collect();

                                reverse.push(10);

                                Datagram { data: reverse, address: bs.address }
                            })
                            .to(sink::detached_logic::DetachedLogicSink::new(UdpSink {
                                actor_ref: ActorRef::empty(),
                                chunk: None,
                                ready: false,
                                socket: sink_socket.try_clone().unwrap(),
                                poll: None,
                                token: 0,
                            }))
                    );
                }
            }

            fn receive(&mut self, msg: (), ctx: &mut ActorContext<()>) {}
        }

        assert!(ActorSystem::new()
            .spawn(MyTestActor)
            .is_ok());
    }
}
