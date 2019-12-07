#[test]
fn test() {
    use crate::actor::{Actor, ActorContext, ActorSystem, Signal};
    use crate::stream::{Flow, Sink, Source};

    struct TestReaper;

    impl Actor for TestReaper {
        type Msg = Vec<u32>;

        fn receive(&mut self, msg: Self::Msg, ctx: &mut ActorContext<Self::Msg>) {
            assert_eq!(msg, vec![10, 11, 12, 20, 21, 22, 30, 31, 32]);

            ctx.stop();
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<Self::Msg>) {
            if let Signal::Started = signal {
                let (_, result) = ctx.spawn(
                    Source::iterator(1..=3)
                        .map(|n| n * 10)
                        .map_concat(|n| vec![n, n + 1, n + 2].into_iter())
                        .via(Flow::new().map_concat(|n| vec![n].into_iter()))
                        .to(Sink::collect()),
                );

                ctx.watch(result, |value| value);
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
