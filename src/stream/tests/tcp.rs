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

    impl Actor for TestReaper {
        type Msg = String;

        fn receive(&mut self, msg: String, ctx: &mut ActorContext<Self::Msg>) {
            assert_eq!(msg, "hello world");

            ctx.stop();
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<Self::Msg>) {
            if let Signal::Started = signal {
                let (source, sink) = Tcp::connect(&"127.0.0.1:2701".parse().unwrap());

                let _ = ctx.spawn(
                    Source::single("hello world".bytes().collect())
                        .to(sink)
                );

                let actor_ref = ctx.actor_ref().clone();

                let _ = ctx.spawn(
                    source
                        .to(Sink::for_each(move |data: Vec<u8>| {
                            let str = str::from_utf8(&data).unwrap().to_owned();
                            actor_ref.tell(str);
                        }))
                );
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper::new()).is_ok());
}

