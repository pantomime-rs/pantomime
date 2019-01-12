//! Configuration

use std::{cmp, env, str};

#[derive(Clone, Debug)]
pub struct ActorSystemConfig {
    pub default_dispatcher_parallelism_min: usize,
    pub default_dispatcher_parallelism_max: usize,
    pub default_dispatcher_parallelism_factor: f32,
    pub default_dispatcher_task_queue_fifo: bool,
    pub log_config_on_start: bool,
    pub num_cpus: usize,
    pub shards_min: usize,
    pub shards_max: usize,
    pub shards_factor: f32,
    pub ticker_interval_ms: u64,
}

// @TODO need to validate

impl ActorSystemConfig {
    pub fn parse() -> Self {
        Self {
            default_dispatcher_parallelism_min: config(
                "PANTOMIME_DEFAULT_DISPATCHER_PARALLELISM_MIN",
                4,
            ),
            default_dispatcher_parallelism_max: config(
                "PANTOMIME_DEFAULT_DISPATCHER_PARALLELISM_MAX",
                64,
            ),
            default_dispatcher_parallelism_factor: config(
                "PANTOMIME_DEFAULT_DISPATCHER_PARALLELISM_FACTOR",
                1.0,
            ),
            default_dispatcher_task_queue_fifo: config(
                "PANTOMIME_DEFAULT_DISPATCHER_TASK_QUEUE_FIFO",
                true,
            ),
            log_config_on_start: config("PANTOMIME_LOG_CONFIG_ON_START", false),
            num_cpus: config("PANTOMIME_NUM_CPUS", num_cpus::get()),
            shards_min: config("PANTOMIME_SHARDS_MIN", 128),
            shards_factor: config("PANTOMIME_SHARDS_FACTOR", 32.0),
            shards_max: config("PANTOMIME_SHARDS_MAX", 2048),
            ticker_interval_ms: config("PANTOMIME_TICKER_INTERVAL_MS", 10),
        }
    }

    pub fn default_dispatcher_parallelism(&self) -> usize {
        cmp::min(
            self.default_dispatcher_parallelism_max,
            cmp::max(
                self.default_dispatcher_parallelism_min,
                (self.num_cpus as f32 * self.default_dispatcher_parallelism_factor) as usize,
            ),
        )
    }

    pub fn shards(&self) -> usize {
        cmp::min(
            self.shards_max,
            cmp::max(
                self.shards_min,
                (self.num_cpus as f32 * self.shards_factor) as usize,
            ),
        )
    }
}

/// A helper function for extracting configuration values
/// from the environment. This can slightly simplify
/// a similar pattern to the above in applications.
pub fn config<T: str::FromStr>(name: &str, default: T) -> T {
    match env::var(name).ok() {
        None => default,

        Some(v) => v.parse().ok().unwrap_or_else(|| {
            warn!("cannot parse {}, using default", name);

            default
        }),
    }
}
