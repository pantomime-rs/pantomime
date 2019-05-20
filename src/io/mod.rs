use crate::actor::ActorRef;
use crate::actor::*;
use ext_mio::{Event, Poll};
use mio::Events;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time;
use std::usize;

const RESERVED_TOKENS: usize = 100;

pub(crate) enum SubscriptionEvent {
    Ready(Arc<Poll>, usize),
    MioEvent(Event),
}

pub(crate) enum IoCoordinatorMsg {
    Subscribe(ActorRef<SubscriptionEvent>),
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
    fn receive(&mut self, msg: IoCoordinatorMsg, _: &mut ActorContext<IoCoordinatorMsg>) {
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
        }
    }
}

pub(crate) struct Poller {
    pub(crate) poll: Arc<Poll>,
}

impl Poller {
    pub(crate) fn new() -> Self {
        // @TODO this should be explicitly modeled...
        let poll = Arc::new(Poll::new().expect("pantomime bug: cannot create Poll"));

        Self { poll }
    }

    pub(crate) fn run(self, actor_ref: &ActorRef<IoCoordinatorMsg>) {
        let poll = self.poll;

        // @TODO revisit stopping

        {
            let actor_ref = actor_ref.clone();
            let poll = poll.clone();

            thread::spawn(move || loop {
                let mut events = Events::with_capacity(1024);

                loop {
                    // @TODO rethink expect below
                    poll.poll_interruptible(&mut events, Some(time::Duration::from_millis(1000)))
                        .expect("pantomime bug: poll_interruptible failed");

                    for event in &events {
                        actor_ref.tell(IoCoordinatorMsg::MioEvent(event));
                    }
                }
            });
        }
    }
}

// @TODO reactivate tests below
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
