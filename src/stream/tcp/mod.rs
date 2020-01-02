use crate::actor::SubscriptionEvent;
use crate::stream::{Action, Logic, LogicEvent, Source, Sink, StreamContext};
use mio::{net::TcpStream, Poll, PollOpt, Ready, Token};
use std::io::{ErrorKind as IoErrorKind, Result as IoResult, Write};
use std::net::SocketAddr;
use std::sync::Arc;

struct TcpConnection {
    source: Source<Vec<u8>>,
    sink: Sink<Vec<u8>, ()>
}

struct Tcp;

impl Tcp {
    fn bind(addr: &SocketAddr) -> Source<TcpConnection> {
        addr.clone();
        //TcpStream::connect(addr)
        unimplemented!()
    }

    fn connect(addr: &SocketAddr) -> TcpConnection {
        unimplemented!()
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
