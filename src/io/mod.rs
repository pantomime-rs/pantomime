use crate::actor::*;
use crate::dispatcher::BoxedFn1In0Out;
use crate::stream::detached::*;
use crate::stream::*;
use ext_mio::{net::UdpSocket, Event, Poll, PollOpt, Ready, Token};
use std::collections::{HashMap, HashSet};
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::usize;

const RESERVED_TOKENS: usize = 100;

pub(crate) enum SubscriptionEvent {
    Ready(Arc<Poll>, usize),
    MioEvent(Event),
}

pub(crate) enum IoCoordinatorMsg {
    Subscribe(ActorRef<SubscriptionEvent>),
    Unsubscribe(usize),
    MioEvent(Event),
}

/// An IoCoordinator bridges the MIO event loop and actors that
/// are dependent upon its events. In practice, these actors
/// are those that power the UDP and TCP streams.
pub(crate) struct IoCoordinator {
    poll: Arc<Poll>,
    subscribers: HashMap<usize, ActorRef<SubscriptionEvent>>,
    last_token_id: usize,
}

impl IoCoordinator {
    pub(crate) fn new(poll: Arc<Poll>) -> Self {
        Self {
            poll,
            subscribers: HashMap::new(),
            last_token_id: RESERVED_TOKENS,
        }
    }

    fn next_token_id(&mut self) -> usize {
        let mut last = self.last_token_id;

        loop {
            // MAX is reserved per mio docs

            let next = if last == usize::MAX - 1 {
                RESERVED_TOKENS + 1
            } else {
                last + 1
            };

            if !self.subscribers.contains_key(&next) {
                return next;
            } else if next == self.last_token_id {
                return 0;
            } else {
                last = next;
            }
        }
    }
}

impl Actor<IoCoordinatorMsg> for IoCoordinator {
    fn receive(&mut self, msg: IoCoordinatorMsg, context: &mut ActorContext<IoCoordinatorMsg>) {
        match msg {
            IoCoordinatorMsg::MioEvent(event) => {
                let token = event.token().0;

                if let Some(subscriber) = self.subscribers.get(&token) {
                    subscriber.tell(SubscriptionEvent::MioEvent(event));
                }
            }

            IoCoordinatorMsg::Subscribe(subscriber) => {
                let next_token_id = self.next_token_id();

                subscriber.tell(SubscriptionEvent::Ready(self.poll.clone(), next_token_id));

                self.subscribers.insert(next_token_id, subscriber);

                self.last_token_id = next_token_id;
            }

            IoCoordinatorMsg::Unsubscribe(id) => {
                // @TODO any need to notify the subscriber?
                self.subscribers.remove(&id);
            }
        }
    }
}

enum BoundUdpMsg {
    /// The UDP socket was successfully bound. The socket will remain bound until
    /// the actor ref is stopped.
    Bound(SystemActorRef),

    /// Data was received via the UDP socket
    Received(Vec<u8>),
}

struct UdpReceived {
    data: Vec<u8>,
}

struct UdpSend {
    data: Vec<u8>,
}

struct UdpBind {
    address: SocketAddr,
    reply_to: ActorRef<BoundUdpMsg>,
}

struct IoUdp {}

impl IoUdp {
    fn new() -> Self {
        Self {}
    }
}

impl Actor<UdpBind> for IoUdp {
    fn receive(&mut self, msg: UdpBind, context: &mut ActorContext<UdpBind>) {
        let bound_udp = context.spawn(BoundUdp::new(msg.reply_to));
    }
}

struct BoundUdp {
    dest: ActorRef<BoundUdpMsg>,
}

impl BoundUdp {
    fn new(dest: ActorRef<BoundUdpMsg>) -> Self {
        Self { dest }
    }
}

impl Actor<BoundUdpMsg> for BoundUdp {
    fn receive(&mut self, msg: BoundUdpMsg, context: &mut ActorContext<BoundUdpMsg>) {}

    fn receive_signal(&mut self, signal: Signal, context: &mut ActorContext<BoundUdpMsg>) {
        match signal {
            Signal::Started => {
                self.dest
                    .tell(BoundUdpMsg::Bound(context.actor_ref().system_ref()));
            }

            _ => (),
        }
    }
}

use crate::actor::ActorRef;
use crossbeam::channel;
use mio::Events;
use std::thread;
use std::time;

enum PollerEvent {
    Stop,
}

pub(crate) struct ActivePoller {
    pub poll: Arc<Poll>,
    sender: channel::Sender<PollerEvent>,
}

impl ActivePoller {
    pub(crate) fn stop(self) {
        if let Some(e) = self.sender.send(PollerEvent::Stop).err() {
            error!("failed to stop poller: {}", e)
        }
    }
}

pub(crate) struct Poller {
    pub poll: Arc<Poll>,
}

impl Poller {
    pub(crate) fn new() -> Self {
        let poll = Arc::new(Poll::new().expect("TODO"));

        Self { poll }
    }

    pub(crate) fn run(self, actor_ref: &ActorRef<IoCoordinatorMsg>) -> ActivePoller {
        let poll = self.poll;

        let (sender, receiver) = channel::unbounded();

        {
            let actor_ref = actor_ref.clone();
            let poll = poll.clone();

            thread::spawn(move || loop {
                let mut events = Events::with_capacity(1024);

                loop {
                    poll.poll_interruptible(&mut events, Some(time::Duration::from_millis(1000)))
                        .expect("TODO");

                    for event in &events {
                        actor_ref.tell(IoCoordinatorMsg::MioEvent(event));
                    }

                    if true {
                        loop {
                            match receiver.try_recv() {
                                Ok(PollerEvent::Stop) => {
                                    println!("done");
                                    return;
                                }

                                Err(channel::TryRecvError::Empty) => {
                                    println!("empty");
                                    break;
                                }

                                Err(channel::TryRecvError::Disconnected) if false => {
                                    // @TODO
                                    println!("disconnected");
                                    return;
                                }

                                _ => {
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        }

        ActivePoller { poll, sender }
    }
}

/*
#[cfg(test)]
mod tests {
    use super::super::poller::Poller;
    use super::*;
    use crate::testkit::*;
    use std::thread;
    use std::time;

    #[test]
    fn test_basic_udp() {
        let mut system = ActorSystem::new().start();

        let poller = Poller::new();

        let io_manager = system.spawn(IoManager::new(poller.poll.clone()));

        let active_poller = poller.run(io_manager.clone());

        let mut probe = system.spawn_probe::<UdpData>();

        io_manager.tell(IoEvent::UdpBind(
            "127.0.0.1:0".parse().unwrap(),
            probe.actor_ref.clone(),
        ));

        let mut port = 0;

        match probe.receive(time::Duration::from_secs(10)) {
            UdpData::Bound(bound_socket) => {
                port = bound_socket.addr.port();
                println!("bound! {:?} {}", bound_socket, port);
            }

            other => panic!("expected UdpData::Bound"),
        }

        thread::sleep(time::Duration::from_millis(500));
        let mut socket = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
        socket
            .send_to(
                b"hello world 1",
                &format!("127.0.0.1:{}", port).parse().unwrap(),
            )
            .unwrap();

        match probe.receive(time::Duration::from_secs(10)) {
            UdpData::DataRead(data) => {
                assert_eq!(String::from_utf8(data).unwrap(), "hello world 1");
            }

            other => panic!("expected UdpData::DataRead"),
        }

        system.context.drain();
    }
}
*/
