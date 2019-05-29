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
