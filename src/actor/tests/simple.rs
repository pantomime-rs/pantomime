use crate::actor::*;

#[test]
fn very_simple_test() {
    struct TestReaper;

    impl Actor for TestReaper {
        type Msg = ();

        fn receive(&mut self, _: Self::Msg, _: &mut ActorContext<Self::Msg>) {}

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<Self::Msg>) {
            if let Signal::Started = signal {
                ctx.stop();
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}

#[test]
fn fairly_simple_test() {
    struct MyActor;
    struct TestReaper;

    impl Actor for MyActor {
        type Msg = ActorRef<()>;

        fn receive(&mut self, msg: Self::Msg, _: &mut ActorContext<Self::Msg>) {
            msg.tell(());
        }
    }

    impl Actor for TestReaper {
        type Msg = ();

        fn receive(&mut self, _: Self::Msg, ctx: &mut ActorContext<Self::Msg>) {
            ctx.stop();
        }

        fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<Self::Msg>) {
            if let Signal::Started = signal {
                ctx.spawn(MyActor).tell(ctx.actor_ref().clone());
            }
        }
    }

    assert!(ActorSystem::new().spawn(TestReaper).is_ok());
}
