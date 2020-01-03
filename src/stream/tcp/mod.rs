use crate::actor::SubscriptionEvent;
use crate::stream::{Action, Logic, LogicEvent, Source, Sink, StreamContext};
use mio::{net::TcpStream, Poll, PollOpt, Ready, Token};
use std::io::{ErrorKind as IoErrorKind, Read, Result as IoResult, Write};
use std::net::SocketAddr;
use std::sync::Arc;

pub struct TcpConnection {
    pub source: Source<Vec<u8>>,
    pub sink: Sink<Vec<u8>, ()>
}

pub struct Tcp;

impl Tcp {
    pub fn bind(addr: &SocketAddr) -> Source<TcpConnection> {
        unimplemented!()
    }

    pub fn connect(addr: &SocketAddr) -> (Source<Vec<u8>>, Sink<Vec<u8>, ()>) {
        match TcpStream::connect(addr) {
            Ok(socket) => {
                (
                    Source::new(TcpSource::new(&socket)),
                    Sink::new(TcpSink::new(&socket))
                )
            }

            Err(err) => {
                panic!("TODO");
            }
        }
    }
}

struct TcpSource {
    socket: IoResult<TcpStream>,
    buffer: Vec<u8>,
    ready: bool,
    waiting: bool,
    poll: Option<Arc<Poll>>,
    token: usize,
}

impl TcpSource {
    fn new(socket: &TcpStream) -> Self {
        Self {
            socket: socket.try_clone(),
            buffer: vec![0; 8192], // @TODO
            ready: false,
            waiting: false,
            poll: None,
            token: 0,
        }
    }

    fn try_read(&mut self) -> Action<Vec<u8>, SubscriptionEvent> {
        match self.socket {
            Ok(ref mut s) if self.ready && self.waiting => {
                match s.read(&mut self.buffer) {
                    Ok(bytes_read) => {
                        self.waiting = false;

                        Action::Push(self.buffer[0..bytes_read].to_vec())
                    }

                    Err(ref e) if e.kind() == IoErrorKind::WouldBlock => {
                        self.ready = false;

                        Action::None
                    }

                    Err(_e) => {
                        // @TODO
                        unimplemented!()
                    }
                }
            }

            _ => Action::None,
        }
    }
}

impl Logic<(), Vec<u8>> for TcpSource {
    type Ctl = SubscriptionEvent;

    fn name(&self) -> &'static str {
        "pantomime::stream::source::Udp"
    }

    fn buffer_size(&self) -> Option<usize> {
        Some(0)
    }

    fn fusible(&self) -> bool {
        false
    }

    fn receive(
        &mut self,
        msg: LogicEvent<(), Self::Ctl>,
        ctx: &mut StreamContext<(), Vec<u8>, Self::Ctl>,
    ) -> Action<Vec<u8>, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => {
                //println!("got pulled");
                self.waiting = true;

                self.try_read()
            }

            LogicEvent::Forwarded(SubscriptionEvent::MioEvent(event)) => {
                //println!("source got a mio event");
                if event.readiness().is_readable() {
                    self.ready = true;

                    self.try_read()
                } else {
                    Action::None
                }
            }

            LogicEvent::Forwarded(SubscriptionEvent::Ready(poll, token)) => {
                //println!("source got a ready");
                match self.socket {
                    Ok(ref socket) => {
                        // @TODO expect doesn't seem appropriate
                        poll.register(socket, Token(token), Ready::readable(), PollOpt::edge())
                            .expect("pantomime bug: failed to register socket");

                        self.poll = Some(poll);
                        self.token = token;

                        self.try_read()
                    }

                    Err(ref _e) => {
                        // shouldn't happen

                        ctx.unsubscribe(token);

                        Action::None
                    }
                }
            }

            LogicEvent::Started => {
                let actor_ref = ctx.stage_ref().actor_ref.clone();

                ctx.subscribe(actor_ref);

                Action::None
            }

            LogicEvent::Stopped => Action::None,

            LogicEvent::Cancelled => {
                //println!(" SOURCE CANCELLED!");
                if let Some(poll) = self.poll.take() {
                    if let Ok(ref socket) = self.socket {
                        // @TODO expect doesn't seem appropriate

                        poll.deregister(socket)
                            .expect("pantomime bug: failed to deregister socket");
                    }
                }

                if self.token > 0 {
                    ctx.unsubscribe(self.token);

                    self.token = 0;
                }

                Action::Stop(None)
            }

            LogicEvent::Pushed(()) => Action::None,
        }
    }
}

struct Chunk {
    data: Vec<u8>,
    written: usize,
}

struct TcpSink {
    socket: IoResult<TcpStream>,
    chunk: Option<Chunk>,
    ready: bool,
    poll: Option<Arc<Poll>>,
    token: usize,
    pulled: bool,
    stopped: bool,
}

impl TcpSink {
    fn new(socket: &TcpStream) -> Self {
        Self {
            socket: socket.try_clone(),
            chunk: None,
            ready: false,
            poll: None,
            token: 0,
            pulled: false,
            stopped: false,
        }
    }

    fn complete(&self) -> Action<(), SubscriptionEvent> {
        if self.pulled {
            Action::PushAndStop((), None)
        } else {
            Action::Stop(None)
        }
    }

    fn try_write(
        &mut self,
        ctx: &mut StreamContext<Vec<u8>, (), SubscriptionEvent>,
    ) -> Action<(), SubscriptionEvent> {
        match (&mut self.socket, &mut self.chunk) {
            (Ok(ref mut s), Some(ref mut chunk)) if self.ready => {
                match s.write(&chunk.data[chunk.written..]) {
                    Ok(bytes_written) => {
                        //println!("wrote {}", bytes_written);
                        chunk.written += bytes_written;

                        if chunk.written == chunk.data.len() {
                            self.chunk = None;

                            if self.stopped {
                                self.unregister(ctx);
                                self.complete()
                            } else {
                                Action::Pull
                            }
                        } else {
                            // @TODO should we call try_write again??
                            //       i.e. call it until exhausted or rxd WouldBlock
                            Action::None
                        }
                    }

                    Err(ref e) if e.kind() == IoErrorKind::WouldBlock => {
                        //println!("would block");
                        self.ready = false;

                        Action::None
                    }

                    Err(_e) => {
                        // @TODO
                        unimplemented!()
                    }
                }
            }

            _ => Action::None,
        }
    }

    fn unregister(&mut self, ctx: &mut StreamContext<Vec<u8>, (), SubscriptionEvent>) {
        if let Some(poll) = self.poll.take() {
            if let Ok(ref socket) = self.socket {
                // @TODO expect doesn't seem appropriate

                poll.deregister(socket)
                    .expect("pantomime bug: failed to deregister socket");
            }
        }

        if self.token > 0 {
            ctx.unsubscribe(self.token);

            self.token = 0;
        }
    }
}

impl Logic<Vec<u8>, ()> for TcpSink {
    type Ctl = SubscriptionEvent;

    fn name(&self) -> &'static str {
        "pantomime::stream::sink::TcpSink"
    }

    fn buffer_size(&self) -> Option<usize> {
        Some(0)
    }

    fn fusible(&self) -> bool {
        false
    }

    fn receive(
        &mut self,
        msg: LogicEvent<Vec<u8>, Self::Ctl>,
        ctx: &mut StreamContext<Vec<u8>, (), Self::Ctl>,
    ) -> Action<(), Self::Ctl> {
        match msg {
            LogicEvent::Pushed(data) => {
                if self.chunk.is_some() {
                    panic!("TODO");
                }
                //println!("got some data ready={:?}", self.ready);

                self.chunk = Some(Chunk { data, written: 0 });

                self.try_write(ctx)
            }

            LogicEvent::Forwarded(SubscriptionEvent::MioEvent(event)) => {
                //println!("sink got an mio event");
                if event.readiness().is_writable() {
                    //println!("is writable");
                    self.ready = true;

                    self.try_write(ctx)
                } else {
                    Action::None
                }
            }

            LogicEvent::Forwarded(SubscriptionEvent::Ready(poll, token)) => {
                //println!("sink is ready");
                match self.socket {
                    Ok(ref socket) => {
                        //println!("registering the socket (token={})", token);
                        // @TODO expect doesn't seem appropriate
                        poll.register(socket, Token(token), Ready::writable(), PollOpt::edge())
                            .expect("pantomime bug: failed to register socket");

                        self.poll = Some(poll);
                        self.token = token;

                        Action::None
                    }

                    Err(ref _e) => {
                        //println!("ERRRR");
                        // shouldn't happen

                        ctx.unsubscribe(token);

                        Action::None
                    }
                }
            }

            LogicEvent::Started => {
                let actor_ref = ctx.stage_ref().actor_ref.clone();

                //println!("sink subscribing for id={}", actor_ref.id());

                //println!("messaging self");

                ctx.subscribe(actor_ref);

                Action::None
            }

            LogicEvent::Pulled => {
                self.pulled = true;
                //println!("will pull");

                Action::Pull
            }

            LogicEvent::Stopped => {
                self.stopped = true;

                if self.chunk.is_none() {
                    self.unregister(ctx);
                    self.complete()
                } else {
                    Action::None
                }
            }

            LogicEvent::Cancelled => {
                self.chunk = None;
                self.unregister(ctx);
                self.complete()
            }
        }
    }
}
