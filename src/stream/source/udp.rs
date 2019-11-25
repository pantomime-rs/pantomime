use crate::actor::SubscriptionEvent;
use crate::stream::{Action, Datagram, Logic, LogicEvent, StreamContext};
use mio::{net::UdpSocket, Poll, PollOpt, Ready, Token};
use std::io::{ErrorKind as IoErrorKind, Result as IoResult};
use std::sync::Arc;

pub struct Udp {
    socket: IoResult<UdpSocket>,
    buffer: Vec<u8>,
    ready: bool,
    waiting: bool,
    poll: Option<Arc<Poll>>,
    token: usize,
}

impl Udp {
    pub fn new(socket: &UdpSocket) -> Self {
        Self {
            socket: socket.try_clone(),
            buffer: vec![0; 8192], // @TODO
            ready: false,
            waiting: false,
            poll: None,
            token: 0,
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
                            address: socket_addr,
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

            _ => Action::None,
        }
    }
}

impl Logic<(), Datagram> for Udp {
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
        ctx: &mut StreamContext<(), Datagram, Self::Ctl>,
    ) -> Action<Datagram, Self::Ctl> {
        match msg {
            LogicEvent::Pulled => {
                println!("got pulled");
                self.waiting = true;

                self.try_read()
            }

            LogicEvent::Forwarded(SubscriptionEvent::MioEvent(event)) => {
                println!("source got a mio event");
                if event.readiness().is_readable() {
                    self.ready = true;

                    self.try_read()
                } else {
                    Action::None
                }
            }

            LogicEvent::Forwarded(SubscriptionEvent::Ready(poll, token)) => {
                println!("source got a ready");
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
                println!(" SOURCE CANCELLED!");
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

                Action::Complete(None)
            }

            LogicEvent::Pushed(()) => Action::None,
        }
    }
}
