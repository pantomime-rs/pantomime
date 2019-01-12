use std::thread;
use std::time::{Duration, Instant};

/// Repeatedly evaluate the provided function upto
/// a specified limit, sleeping for 10ms between
/// executions.
///
/// If the function doesn't return true within the
/// limit, this panics and thus fails the test.
///
/// This is useful for testing asynchronous behavior
/// from different threads in a polling fashion.
pub fn eventually<F: FnMut() -> bool>(limit: Duration, mut f: F) {
    let start = Instant::now();

    while !f() {
        if start.elapsed() > limit {
            panic!("provided function hasn't returned true within {:?}", limit);
        }

        thread::sleep(Duration::from_millis(30));
    }
}
