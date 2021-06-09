use crate::stream::*;
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

    enum TestMsg {
        Spawn(Stream<()>),
        Received(String)
    }

    // @TODO fix framing in server impl
    // @TODO expose connecting client's ip to server impl
    // @TODO expose bound socket to server impl
    // @TODO wire together the client test and server test (currently manually verified by telnet)
    impl Actor for TestReaper {
        type Msg = TestMsg;

        fn receive(&mut self, msg: TestMsg, ctx: &mut ActorContext<Self::Msg>) {
            match msg {
                TestMsg::Spawn(stream) => {
                    ctx.spawn(stream);
                }

                TestMsg::Received(msg) => {
                    //assert_eq!(msg, "hello world");

                    //ctx.stop();
                }
            }
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<Self::Msg>) {
            if let Signal::Started = signal {
                let actor_ref = ctx.actor_ref().clone();

                ctx.spawn(
                    Tcp::bind(&"127.0.0.1:2708".parse().unwrap())
                        .to(Sink::for_each(move |cx: TcpConnection| {
                            actor_ref.tell(
                                TestMsg::Spawn(
                                    cx.source
                                        .map(|data| {
                                            // @TODO this actually needs framing to be correct

                                            let data = str::from_utf8(&data).unwrap().chars().rev().collect::<String>();

                                            data.into()
                                        })
                                        .to(cx.sink)
                                )
                            );
                        }))
                );

                let cx = Tcp::connect(&"127.0.0.1:2701".parse().unwrap());

                let _ = ctx.spawn(
                    Source::single("hello world".bytes().collect())
                        .to(cx.sink)
                );

                let actor_ref = ctx.actor_ref().clone();

                let _ = ctx.spawn(
                    cx.source
                        .to(Sink::for_each(move |data: Vec<u8>| {
                            let str = str::from_utf8(&data).unwrap().to_owned();
                            actor_ref.tell(TestMsg::Received(str));
                        }))
                );
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}

