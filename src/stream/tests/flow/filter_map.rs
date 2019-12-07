#[test]
fn test() {
    use crate::actor::{Actor, ActorContext, ActorSystem, Signal};
    use crate::stream::{Flow, Sink, Source};

    struct TestReaper;

    impl Actor for TestReaper {
        type Msg = Vec<u32>;

        fn receive(&mut self, msg: Self::Msg, ctx: &mut ActorContext<Self::Msg>) {
            assert_eq!(msg, vec![17, 34, 51, 68, 85]);

            ctx.stop();
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<Self::Msg>) {
            if let Signal::Started = signal {
                let (_, result) = ctx.spawn(
                    Source::iterator(1..=100)
                        .filter_map(|n| if n % 17 == 0 { Some(n as u32) } else { None })
                        .via(Flow::new().filter_map(Some))
                        .to(Sink::collect()),
                );

                ctx.watch(result, |value| value);
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
