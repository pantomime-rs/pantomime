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
    pub process_exit: bool,
    pub shards_min: usize,
    pub shards_max: usize,
    pub shards_factor: f32,

    pub ticker_interval_ms: u64,

    /// A list of signals that Pantomime will install handlers for.
    ///
    /// These should be comma separated and are parsed to a reasonable
    /// degree, i.e. whitespace is trimmed, the value is uppercased.
    #[cfg(feature = "posix-signals-support")]
    pub posix_signals: Vec<i32>,

    /// A list of signals (e.g. SIGTERM) that will cause Pantomime
    /// to drain the system and shutdown.
    ///
    /// If a signal is listed here but not in posix_signals, it will
    /// have no effect.
    ///
    /// These should be comma separated and are parsed to a reasonable
    /// degree, i.e. whitespace is trimmed, the value is uppercased.
    #[cfg(feature = "posix-signals-support")]
    pub posix_shutdown_signals: Vec<i32>,
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
            process_exit: config("PANTOMIME_PROCESS_EXIT", true),
            shards_min: config("PANTOMIME_SHARDS_MIN", 128),
            shards_factor: config("PANTOMIME_SHARDS_FACTOR", 32.0),
            shards_max: config("PANTOMIME_SHARDS_MAX", 2048),
            ticker_interval_ms: config("PANTOMIME_TICKER_INTERVAL_MS", 10),

            #[cfg(feature = "posix-signals-support")]
            posix_signals: parse_posix_signals(config(
                "PANTOMIME_POSIX_SIGNALS",
                "SIGINT,SIGTERM,SIGHUP,SIGUSR1,SIGUSR2".to_string(),
            )),

            #[cfg(feature = "posix-signals-support")]
            posix_shutdown_signals: parse_posix_signals(config(
                "PANTOMIME_POSIX_EXIT_SIGNALS",
                "SIGINT,SIGTERM".to_string(),
            )),
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

#[cfg(feature = "posix-signals-support")]
fn parse_posix_signals(list: String) -> Vec<i32> {
    let mut signals = Vec::new();

    for sig in list.split(",").map(|s| s.trim().to_uppercase()) {
        let signal = match sig.as_str() {
            "SIGHUP" => crate::posix_signals::SIGHUP,
            "SIGINT" => crate::posix_signals::SIGINT,
            "SIGUSR1" => crate::posix_signals::SIGUSR1,
            "SIGUSR2" => crate::posix_signals::SIGUSR2,
            "SIGTERM" => crate::posix_signals::SIGTERM,
            _ => 0,
        };

        if signal != 0 {
            signals.push(signal);
        }
    }

    signals
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        assert_eq!(config("_NOT_SET", 10), 10);
    }

    #[test]
    #[cfg(feature = "posix-signals-support")]
    fn test_parse_posix_signals() {
        assert_eq!(parse_posix_signals("".to_string()), vec![]);
        assert_eq!(parse_posix_signals("SIGINT".to_string()), vec![2]);
        assert_eq!(parse_posix_signals("sigint".to_string()), vec![2]);
        assert_eq!(parse_posix_signals(" sigint ".to_string()), vec![2]);

        assert_eq!(parse_posix_signals(" unknown ".to_string()), vec![]);

        assert_eq!(
            parse_posix_signals("SIGHUP,SIGINT,SIGUSR1,SIGUSR2,SIGTERM,UNKNOWN".to_string()),
            vec![1, 2, 10, 12, 15]
        );
    }
}
