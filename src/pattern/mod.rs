//! Contains common patterns for interacting with actors

#[cfg(feature = "futures-support")]
mod ask;

#[cfg(feature = "futures-support")]
mod pipe;

#[cfg(feature = "futures-support")]
pub use self::ask::Ask;

#[cfg(feature = "futures-support")]
pub use self::pipe::PipeTo;
