#[cfg(target_family = "unix")]
extern crate signal_hook;

#[cfg(target_family = "unix")]
pub use signal_hook::*;
