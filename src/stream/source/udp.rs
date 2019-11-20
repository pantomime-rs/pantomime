use crate::actor::SubscriptionEvent;
use crate::stream::{Action, Logic, LogicEvent, StreamContext};
use mio::{net::UdpSocket, Poll, PollOpt, Ready, Token};
use std::io::{ErrorKind as IoErrorKind, Result as IoResult};
use std::net::SocketAddr;
use std::sync::Arc;

pub struct Datagram {
    pub data: Vec<u8>,
    pub address: SocketAddr
}

pub struct Udp {
    socket: IoResult<UdpSocket>,
    buffer: Vec<u8>,
    ready: bool,
    waiting: bool,
    poll: Option<Arc<Poll>>,
    token: usize,
}

impl Udp {
    pub fn new(address: &SocketAddr) -> Self {
        Self {
            socket: UdpSocket::bind(address),
            buffer: vec![0; 8192], // @TODO
            ready: false,
            waiting: false,
            poll: None,
            token: 0
        }
    }

    fn try_read(&mut self) -> Action<Datagram, SubscriptionEvent> {
        match self.socket {
            Ok(ref mut s) if self.ready && self.waiting => {
                match s.recv_from(&mut self.buffer) {
                    Ok((bytes_read, socket_addr)) => {
                        self.waiting = false;

                        Action::Push(Datagram {
                            data: self.buffer[0..bytes_read].to_vec(),
                            address: socket_addr
                        })
                    }

                    Err(e) if e.kind() == IoErrorKind::WouldBlock => {
                        self.ready = false;

                        Action::None
                    }

                    Err(_e) => {
                        // @TODO
                        unimplemented!()
                    }
                }
            }

            _ => {
                Action::None
            }
        }
    }
}

impl Logic<(), Datagram> for Udp {
    type Ctl = SubscriptionEvent;

    fn name(&self) -> &'static str {
        "Udp"
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
        ctx: &mut StreamContext<(), Datagram, Self::Ctl>,
    ) -> Action<Datagram, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => {
                self.waiting = true;

                self.try_read()
            }

            LogicEvent::Forwarded(SubscriptionEvent::MioEvent(event)) => {
                if event.readiness().is_readable() {
                    self.ready = true;

                    self.try_read()
                } else {
                    Action::None
                }
            }

            LogicEvent::Forwarded(SubscriptionEvent::Ready(poll, token)) => {
                match self.socket {
                    Ok(ref socket) => {
                        // @TODO expect doesn't seem appropriate
                        poll.register(
                            socket,
                            Token(token),
                            Ready::readable(),
                            PollOpt::edge(),
                        )
                        .expect("pantomime bug: failed to register socket");

                        self.poll = Some(poll);
                        self.token = token;

                        Action::None
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

            LogicEvent::Cancelled => {
                unimplemented!()
            }

            LogicEvent::Pushed(()) | LogicEvent::Stopped => Action::None,
        }
    }
}
