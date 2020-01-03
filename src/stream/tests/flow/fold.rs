#[test]
fn test() {
    use crate::actor::{Actor, ActorContext, ActorSystem, Signal};
    use crate::stream::{Flow, Sink, Source};

    struct TestReaper;

    impl Actor for TestReaper {
        type Msg = Option<Vec<usize>>;

        fn receive(&mut self, msg: Self::Msg, ctx: &mut ActorContext<Self::Msg>) {
            assert_eq!(msg, Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));

            ctx.stop();
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<Self::Msg>) {
            if let Signal::Started = signal {
                let (_, result) = ctx.spawn(
                    Source::iterator(1..=10)
                        .fold(Vec::new(), |mut a, n| {
                            a.push(n);
                            a
                        })
                        .via(Flow::new().fold(Vec::new(), |_, n| n))
                        .to(Sink::first()),
                );

                ctx.watch(result, |value| value);
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
