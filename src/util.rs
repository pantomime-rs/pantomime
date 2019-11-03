use crate::dispatcher::BoxedFn;

const CUTE_RING_BUFFER_SIZE: usize = 4;

/// A naive ring buffer implementation, as in
/// "aww, that's so cute" (condescending)
pub(in crate) struct CuteRingBuffer<A> {
    front: usize,
    back: usize,
    data: [Option<A>; CUTE_RING_BUFFER_SIZE],
    len: usize,
}

impl<A: Sized> CuteRingBuffer<A> {
    pub fn new() -> Self {
        Self {
            data: [None, None, None, None],
            front: 0,
            back: 0,
            len: 0,
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.len == CUTE_RING_BUFFER_SIZE
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn pop(&mut self) -> A {
        //assert!(self.len > 0, "cannot pop from empty buffer");

        let front = self.front;

        self.front = (front + 1) % CUTE_RING_BUFFER_SIZE;
        self.len -= 1;
        self.data[front].take().unwrap()
    }

    #[inline(always)]
    pub fn push(&mut self, el: A) {
        //assert!(self.len < CUTE_RING_BUFFER_SIZE, "cannot push into full buffer");

        self.data[self.back] = Some(el);
        self.back = (self.back + 1) % CUTE_RING_BUFFER_SIZE;
        self.len += 1;
    }
}

pub struct Deferred {
    thunk: Option<Box<dyn BoxedFn + 'static>>,
}

impl Deferred {
    pub fn new<F: FnOnce()>(f: F) -> Self
    where
        F: 'static,
    {
        Self {
            thunk: Some(Box::new(f)),
        }
    }
}

impl Drop for Deferred {
    fn drop(&mut self) {
        if let Some(thunk) = self.thunk.take() {
            thunk.apply();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::util::{CuteRingBuffer, Deferred};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_cute_ring_buffer() {
        let mut buf = CuteRingBuffer::new();

        assert!(buf.is_empty());
        assert!(!buf.is_full());
        assert_eq!(buf.len(), 0);

        buf.push(10);
        buf.push(20);

        assert!(!buf.is_full());
        assert!(!buf.is_empty());
        assert_eq!(buf.len(), 2);

        assert_eq!(buf.pop(), 10);
        assert_eq!(buf.pop(), 20);

        assert!(buf.is_empty());
        assert!(!buf.is_full());
        assert_eq!(buf.len(), 0);

        buf.push(30);
        buf.push(40);
        buf.push(50);
        buf.push(60);

        assert!(buf.is_full());
        assert!(!buf.is_empty());
        assert_eq!(buf.len(), 4);

        assert_eq!(buf.pop(), 30);

        assert!(!buf.is_full());
        assert!(!buf.is_empty());
        assert_eq!(buf.len(), 3);

        assert_eq!(buf.pop(), 40);
        assert_eq!(buf.pop(), 50);
        assert_eq!(buf.pop(), 60);

        assert!(buf.is_empty());
        assert!(!buf.is_full());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    #[should_panic]
    fn test_cute_ring_buffer_panics_one() {
        let mut buf = CuteRingBuffer::new();

        buf.push(1);

        buf.pop();
        buf.pop();
    }

    #[test]
    #[should_panic]
    fn test_cute_ring_buffer_panics_two() {
        let mut buf = CuteRingBuffer::new();

        buf.push(1);
        buf.push(1);
        buf.push(1);
        buf.push(1);
        buf.push(1);
    }

    #[test]
    fn test_deferred() {
        let done = Arc::new(AtomicBool::new(false));

        {
            let done = done.clone();

            let _ = thread::spawn(move || {
                #[allow(unused_variables)]
                let deferred = {
                    let done = done.clone();

                    Deferred::new(move || {
                        done.store(true, Ordering::SeqCst);
                    })
                };

                assert!(!done.load(Ordering::SeqCst));

                panic!();
            })
            .join();
        }

        assert!(done.load(Ordering::SeqCst));
    }
}
