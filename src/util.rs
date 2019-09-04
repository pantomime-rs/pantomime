use crate::dispatcher::BoxedFn;

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
    use crate::util::Deferred;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;

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
