use crate::stream::internal::{FlowWithFlow, FlowWithSink, LogicContainer, LogicContainerFacade};
use crate::stream::{Logic, Sink, Source};
use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;

mod delay;
mod filter;
mod identity;
mod map;
mod scan;
mod take_while;

pub use self::delay::Delay;
pub use self::filter::Filter;
pub use self::identity::Identity;
pub use self::map::Map;
pub use self::scan::Scan;

pub struct Flow<A, B> {
    pub(in crate::stream) logic: Box<LogicContainerFacade<A, B> + Send>,
    pub(in crate::stream) empty: bool,
    pub(in crate::stream) fused: bool,
}

impl<A, B> Flow<A, B>
where
    A: 'static + Send,
    B: 'static + Send,
{
    pub fn from_logic<L: Logic<A, B>>(logic: L) -> Self
    where
        L: 'static + Send,
        L::Ctl: 'static + Send,
    {
        Self {
            logic: Box::new(LogicContainer {
                logic,
                phantom: PhantomData,
            }),
            empty: false,
            fused: false,
        }
    }

    pub fn to<Out: Send>(self, sink: Sink<B, Out>) -> Sink<A, Out> {
        let fused = sink.fused;

        Sink {
            logic: Box::new(FlowWithSink { flow: self, sink }),
            fused,
            phantom: PhantomData,
        }
    }

    pub fn to_fused<Out: Send>(self, mut sink: Sink<B, Out>) -> Sink<A, Out> {
        self.to_fused(sink)
    }

    pub fn via<C>(self, flow: Flow<B, C>) -> Flow<A, C>
    where
        C: 'static + Send,
    {
        if self.empty && flow.empty {
            cast(flow).expect("pantomime bug: stream::flow::cast failure")
        } else if self.empty {
            // we know that A and B are the same

            cast(flow).expect("pantomime bug: stream::flow::cast failure")
        } else if flow.empty {
            // we know that B and C are the same

            cast(self).expect("pantomime bug: stream::flow::cast failure")
        } else {
            Flow {
                logic: Box::new(FlowWithFlow {
                    one: self,
                    two: flow,
                }),
                empty: false,
                fused: false,
            }
        }
    }

    pub fn via_fused<C>(self, mut flow: Flow<B, C>) -> Flow<A, C>
    where
        C: 'static + Send,
    {
        flow.fused = true;

        self.via(flow)
    }

    pub fn scan<C, F: FnMut(C, B) -> C>(self, zero: C, scan_fn: F) -> Flow<A, C>
    where
        F: 'static + Send,
        C: 'static + Clone + Send,
    {
        self.via(Flow::from_logic(Scan::new(zero, scan_fn)))
    }

    pub fn filter<F: FnMut(&B) -> bool>(self, filter: F) -> Self
    where
        F: 'static + Send,
    {
        self.via(Flow::from_logic(Filter::new(filter)))
    }

    pub fn map<C, F: FnMut(B) -> C>(self, map_fn: F) -> Flow<A, C>
    where
        C: 'static + Send,
        F: 'static + Send,
    {
        self.via(Flow::from_logic(Map::new(map_fn)))
    }
}

fn cast<In: 'static, Out: 'static>(value: In) -> Option<Out> {
    let cell = RefCell::new(Some(value));
    let cell = &cell as &dyn Any;
    let cell = cell.downcast_ref::<RefCell<Option<Out>>>()?;
    let result = cell.borrow_mut().take()?;

    Some(result)
}

impl<A> Flow<A, A>
where
    A: 'static + Send,
{
    pub fn new() -> Self {
        let mut flow = Flow::from_logic(Identity);
        flow.empty = true;
        flow
    }
}

impl<A> Flow<(), A>
where
    A: 'static + Send,
{
    pub fn from_source(source: Source<A>) -> Self {
        unimplemented!()
    }
}

struct Specialize;

impl Specialize {
    fn into<In: 'static, Out: 'static>(value: In) -> Result<Out, In> {
        let cell = RefCell::new(Some(value));
        let casted_cell = &cell as &dyn Any;

        match casted_cell.downcast_ref::<RefCell<Option<Out>>>() {
            Some(cell) => Ok(cell.borrow_mut().take().unwrap()),
            None => Err(cell.borrow_mut().take().unwrap()),
        }
    }
}

struct Contained<T: 'static> {
    value: Box<T>,
}

impl<T: 'static> Contained<T> {
    fn new(value: T) -> Self {
        Self {
            value: Box::new(value),
        }
    }

    fn from_box(value: Box<T>) -> Self {
        Self { value }
    }

    fn conceal(self) -> Concealed {
        Concealed { value: self.value }
    }

    fn into<U: 'static>(self) -> Result<U, T> {
        match self.into_box() {
            Ok(v) => Ok(*v),
            Err(e) => Err(*e),
        }
    }

    fn into_box<U: 'static>(self) -> Result<Box<U>, Box<T>> {
        Specialize::into(self.value)
    }
}

trait ConcealedValue {}

impl<T> ConcealedValue for T {}

struct Concealed {
    value: Box<dyn ConcealedValue>,
}

impl Concealed {
    fn reveal<T: 'static>(self) -> Result<T, Concealed> {
        Err(self)
    }
}

#[test]
fn test_cast() {
    use std::fmt::Debug;

    struct A<T: 'static + PartialEq> {
        value: T,
    };

    impl<T: 'static + PartialEq + Debug> A<T> {
        fn test<U: 'static + PartialEq + Debug>(self, value: U) {
            assert_eq!(cast(self).map(|a: A<U>| a.value), Some(value));
        }
    }

    (A { value: 42 }).test(42);

    assert_eq!(Specialize::into::<usize, usize>(42), Ok(42));
    assert_eq!(Specialize::into::<usize, f64>(42), Err(42));

    fn test<T: 'static + PartialEq + Debug>(value: A<T>, expected: i32) {
        let result: Result<A<i32>, A<T>> = Specialize::into(value);

        match result {
            Ok(a) => {
                assert_eq!(a.value, expected);
            }

            Err(value) => {
                panic!("didn't work!");
            }
        }
    }

    test(A { value: 42 }, 42);
}
