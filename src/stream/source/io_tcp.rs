use crate::actor::*;
use crate::io::*;
use crate::stream::detached::*;
use crate::stream::*;
use ext_mio::{tcp::*, Poll, PollOpt, Ready, Token};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::usize;

#[derive(Debug)]
pub struct Payload {
    pub data: Vec<u8>,
}

struct TcpSource {
    actor_ref: ActorRef<AsyncAction<Payload, TcpSourceMsg>>,
    buffer: Vec<u8>,
    ready: bool,
    socket: TcpStream,
    waiting: bool,
    poll: Option<Arc<Poll>>,
    token: usize,
}

enum TcpSourceMsg {
    SubscriptionEvent(SubscriptionEvent),
}

impl TcpSource {
    fn try_read(&mut self) -> Option<AsyncAction<Payload, TcpSourceMsg>> {
        if self.ready && self.waiting {
            match self.socket.read(&mut self.buffer) {
                Ok(bytes_read) => {
                    self.waiting = false;
                    // @TODO verify buffer logic, perf

                    Some(AsyncAction::Push(Payload {
                        data: self.buffer[0..bytes_read].to_vec(),
                    }))
                }

                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    self.ready = false;
                    None
                }

                Err(e) => {
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
impl DetachedLogic<(), Payload, TcpSourceMsg> for TcpSource {
    fn attach(
        &mut self,
        ctx: &StreamContext,
        actor_ref: &ActorRef<AsyncAction<Payload, TcpSourceMsg>>,
    ) -> Option<AsyncAction<Payload, TcpSourceMsg>> {
        let io_coordinator_ref = ctx
            .system_context()
            .io_coordinator_ref()
            .expect("pantomime bug: IoCoordinator not present");

        io_coordinator_ref.tell(IoCoordinatorMsg::Subscribe(
            actor_ref.convert(|s| AsyncAction::Forward(TcpSourceMsg::SubscriptionEvent(s))),
        ));

        self.actor_ref = actor_ref.clone();

        None
    }

    fn forwarded(&mut self, msg: TcpSourceMsg) -> Option<AsyncAction<Payload, TcpSourceMsg>> {
        match msg {
            TcpSourceMsg::SubscriptionEvent(SubscriptionEvent::MioEvent(event)) => {
                if event.readiness().is_readable() {
                    self.ready = true;
                }
            }

            TcpSourceMsg::SubscriptionEvent(SubscriptionEvent::Ready(poll, token)) => {
                poll.register(
                    &self.socket,
                    Token(token),
                    Ready::readable(),
                    PollOpt::edge(),
                )
                .unwrap();

                self.poll = Some(poll);
                self.token = token;
            }
        }

        self.try_read()
    }

    fn produced(&mut self, _: ()) -> Option<AsyncAction<Payload, TcpSourceMsg>> {
        None
    }

    fn pulled(&mut self) -> Option<AsyncAction<Payload, TcpSourceMsg>> {
        self.waiting = true;

        // note that there's nothing to pull from, as upstream is
        // always Disconnected

        self.try_read()
    }

    fn completed(&mut self) -> Option<AsyncAction<Payload, TcpSourceMsg>> {
        println!("completed!");

        Some(AsyncAction::Complete)
    }

    fn failed(&mut self, _: Error) -> Option<AsyncAction<Payload, TcpSourceMsg>> {
        // @TODO
        None
    }
}

enum TcpSinkMsg {
    SubscriptionEvent(SubscriptionEvent),
}

struct Chunk {
    data: Payload,
    written: usize,
}

struct TcpSink {
    actor_ref: ActorRef<AsyncAction<(), TcpSinkMsg>>,
    chunk: Option<Chunk>,
    ready: bool,
    stream: TcpStream,
    poll: Option<Arc<Poll>>,
    token: usize,
}

impl TcpSink {
    fn try_write(&mut self) -> Option<AsyncAction<(), TcpSinkMsg>> {
        if self.ready && self.chunk.is_some() {
            if let Some(ref mut chunk) = self.chunk {
                match self.stream.write(&chunk.data.data[chunk.written..]) {
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

                    Err(e) => {
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

impl DetachedLogic<Payload, (), TcpSinkMsg> for TcpSink {
    fn attach(
        &mut self,
        ctx: &StreamContext,
        actor_ref: &ActorRef<AsyncAction<(), TcpSinkMsg>>,
    ) -> Option<AsyncAction<(), TcpSinkMsg>> {
        let io_coordinator_ref = ctx
            .system_context()
            .io_coordinator_ref()
            .expect("pantomime bug: IoCoordinator not present");

        io_coordinator_ref.tell(IoCoordinatorMsg::Subscribe(
            actor_ref.convert(|s| AsyncAction::Forward(TcpSinkMsg::SubscriptionEvent(s))),
        ));

        self.actor_ref = actor_ref.clone();

        None
    }

    fn forwarded(&mut self, msg: TcpSinkMsg) -> Option<AsyncAction<(), TcpSinkMsg>> {
        match msg {
            TcpSinkMsg::SubscriptionEvent(SubscriptionEvent::MioEvent(event)) => {
                if event.readiness().is_writable() {
                    self.ready = true;
                }
            }

            TcpSinkMsg::SubscriptionEvent(SubscriptionEvent::Ready(poll, token)) => {
                poll.register(
                    &self.stream,
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

    fn produced(&mut self, data: Payload) -> Option<AsyncAction<(), TcpSinkMsg>> {
        if self.chunk.is_some() {
            panic!();
        }

        self.chunk = Some(Chunk { data, written: 0 });

        self.try_write()
    }

    fn pulled(&mut self) -> Option<AsyncAction<(), TcpSinkMsg>> {
        Some(AsyncAction::Pull)
    }

    fn completed(&mut self) -> Option<AsyncAction<(), TcpSinkMsg>> {
        if let Err(e) = self.stream.shutdown(Shutdown::Write) {
            // @TODO
        } else {
            println!("closed write");
        }

        Some(AsyncAction::Complete)
    }

    fn failed(&mut self, error: Error) -> Option<AsyncAction<(), TcpSinkMsg>> {
        if let Err(e) = self.stream.shutdown(Shutdown::Write) {
            // @TODO
        }

        Some(AsyncAction::Fail(error))
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::Poller;
    use crate::stream::*;
    use crate::testkit::*;
    use std::thread;
    use std::time;

    #[test]
    fn test_basic_tcp() {
        let mut system = ActorSystem::new().start();

        let socket_in = TcpStream::connect(&"127.0.0.1:2701".parse().unwrap()).unwrap();

        let socket_out = socket_in.try_clone().unwrap();

        let tcp_source = TcpSource {
            actor_ref: ActorRef::empty(),
            buffer: vec![0; 8192],
            ready: false,
            socket: socket_in,
            waiting: false,
            poll: None,
            token: 0,
        };

        let tcp_sink = TcpSink {
            actor_ref: ActorRef::empty(),
            chunk: None,
            ready: false,
            stream: socket_out,
            poll: None,
            token: 0,
        };

        let mut n = 0;
        let mut o = 0;

        Sources::empty()
            .via(Detached::new_sized(tcp_source, 0))
            .map(move |payload| {
                n += 1;

                println!("{}", n);

                Payload { data: payload.data }
            })
            .take_while(move |&_| {
                o += 1;

                o < 10
            })
            .via(Detached::new(tcp_sink))
            .run_with(Sinks::ignore(), &system.context);

        loop {
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }

        system.context.drain();
    }
}
*/
