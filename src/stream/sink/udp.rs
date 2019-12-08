use crate::actor::SubscriptionEvent;
use crate::stream::{Action, Datagram, Logic, LogicEvent, StreamContext};
use mio::{net::UdpSocket, Poll, PollOpt, Ready, Token};
use std::io::{ErrorKind as IoErrorKind, Result as IoResult};
use std::sync::Arc;

struct Chunk {
    data: Datagram,
    written: usize,
}

pub struct Udp {
    socket: IoResult<UdpSocket>,
    chunk: Option<Chunk>,
    ready: bool,
    poll: Option<Arc<Poll>>,
    token: usize,
    pulled: bool,
    stopped: bool,
}
impl Udp {
    pub fn new(socket: &UdpSocket) -> Self {
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
        ctx: &mut StreamContext<Datagram, (), SubscriptionEvent>,
    ) -> Action<(), SubscriptionEvent> {
        match (&mut self.socket, &mut self.chunk) {
            (Ok(ref mut s), Some(ref mut chunk)) if self.ready => {
                match s.send_to(&chunk.data.data[chunk.written..], &chunk.data.address) {
                    Ok(bytes_written) => {
                        //println!("wrote {}", bytes_written);
                        chunk.written += bytes_written;

                        if chunk.written == chunk.data.data.len() {
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

    fn unregister(&mut self, ctx: &mut StreamContext<Datagram, (), SubscriptionEvent>) {
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

impl Logic<Datagram, ()> for Udp {
    type Ctl = SubscriptionEvent;

    fn name(&self) -> &'static str {
        "pantomime::stream::sink::Udp"
    }

    fn buffer_size(&self) -> Option<usize> {
        Some(0)
    }

    fn fusible(&self) -> bool {
        false
    }

    fn receive(
        &mut self,
        msg: LogicEvent<Datagram, Self::Ctl>,
        ctx: &mut StreamContext<Datagram, (), Self::Ctl>,
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
