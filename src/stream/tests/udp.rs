use crate::stream::*;
use mio::net::UdpSocket;
use std::str;

#[test]
fn basic_test() {
    use crate::actor::*;
    use crate::stream::{Sink, Source};

    struct TestReaper;

    impl TestReaper {
        fn new() -> Self {
            Self
        }
    }

    impl Actor for TestReaper {
        type Msg = Option<Datagram>;

        fn receive(&mut self, msg: Self::Msg, ctx: &mut ActorContext<Self::Msg>) {
            let msg = msg.unwrap();
            let msg = str::from_utf8(&msg.data).unwrap();

            assert_eq!(msg, "12345");

            ctx.stop();
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<Self::Msg>) {
            if let Signal::Started = signal {
                let socket = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
                let addr = socket.local_addr().unwrap();
                let source = Source::udp(&socket);

                let (_, result) = ctx.spawn(source.to(Sink::first()));
                ctx.watch(result, |value| value);

                let _ = ctx.spawn(
                    Source::single(Datagram::new(b"12345".to_vec(), addr)).to(Sink::udp(&socket)),
                );
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}
