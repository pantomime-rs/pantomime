//! Configuration

use std::collections::HashMap;
use std::{borrow, env, fmt, io, str};

/// A `Config` holds simple key/value pairings that are sourced
/// from a few layers, and provides methods to extract values.
///
/// A focus is on ergonomics, so copies are frequently employed. It
/// is intended to only be used during bootstrapping the application,
/// allowing developers to specify values at a few different layers.
///
/// Configuration values are layered, where by the environment
/// variables take highest precedence, followed by the application's
/// specified defaults (if any), followed by the libraries fallback
/// defaults.
#[derive(Clone)]
pub struct Config {
    defaults: HashMap<String, String>,
}

impl Config {
    /// Create a new configuration with the specified defaults. These
    /// defaults are used for extracting configuration values if they
    /// are not defined in the environment.
    pub fn new(defaults: &[(&str, &str)]) -> Config {
        let defaults = {
            let mut map = HashMap::new();

            for (key, value) in defaults.iter() {
                map.insert(key.to_string(), value.to_string());
            }

            map
        };

        Config { defaults }
    }

    /// Create a new configuration with the specified fallback defaults. That is,
    /// they only take effect if not defined by the environment or already supplied
    /// defaults.
    pub fn with_fallback(&self, fallback_defaults: &[(&str, &str)]) -> Config {
        let mut cfg = Config {
            defaults: self.defaults.clone(),
        };

        for (key, value) in Self::new(fallback_defaults).defaults.into_iter() {
            cfg.defaults.entry(key).or_insert(value);
        }

        cfg
    }

    pub fn parsed<T: str::FromStr>(&self, name: &str) -> io::Result<T>
    where
        T::Err: fmt::Display,
    {
        let provided_result = env::var(&name)
            .ok()
            .or_else(|| self.defaults.get(name).map(borrow::ToOwned::to_owned))
            .map(|s| s.parse::<T>());

        match provided_result {
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("config missing: {}", name),
            )),
            Some(Ok(value)) => Ok(value),
            Some(Err(e)) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("config parse error: {} {}", name, e),
            )),
        }
    }

    pub fn parsed_vec<T: str::FromStr>(&self, name: &str) -> io::Result<Vec<T>>
    where
        T::Err: fmt::Display,
    {
        self.string_vec(&name).and_then(|values| {
            values
                .into_iter()
                .map(|v| {
                    v.parse::<T>().map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("config parse error: {} {}", name, e),
                        )
                    })
                })
                .collect()
        })
    }

    pub fn string_vec(&self, name: &str) -> io::Result<Vec<String>> {
        self.string(name).map(|value| {
            let mut arg = String::new();
            let mut args = Vec::new();
            let mut escaped = false;

            for c in value.chars() {
                if escaped {
                    escaped = false;
                    arg.push(c);
                } else if c == '\\' {
                    escaped = true;
                } else if c == ',' {
                    args.push(arg);
                    arg = String::new();
                } else {
                    arg.push(c);
                }
            }

            if !value.is_empty() {
                args.push(arg);
            }

            args
        })
    }

    pub fn string(&self, name: &str) -> io::Result<String> {
        env::var(name)
            .ok()
            .or_else(|| self.defaults.get(name).map(borrow::ToOwned::to_owned))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, format!("config missing: {}", name))
            })
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            defaults: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ActorSystemConfig {
    pub default_actor_throughput: usize,
    pub default_dispatcher_logic: String,
    pub default_dispatcher_logic_work_stealing_parallelism_min: usize,
    pub default_dispatcher_logic_work_stealing_parallelism_max: usize,
    pub default_dispatcher_logic_work_stealing_parallelism_factor: f32,
    pub default_dispatcher_logic_work_stealing_task_queue_fifo: bool,
    pub default_mailbox_logic: String,
    pub log_config_on_start: bool,
    pub mio_event_capacity: usize,
    pub mio_poll_error_delay_ms: u64,
    pub num_cpus: usize,
    pub process_exit: bool,

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
    #[rustfmt::skip]
    pub fn new(cfg: &Config) -> io::Result<Self> {
        #[cfg(feature = "posix-signals-support")]
        fn posix_signal(sig: String) -> i32 {
            match sig.as_str() {
                "SIGHUP"  => crate::posix_signals::SIGHUP,
                "SIGINT"  => crate::posix_signals::SIGINT,
                "SIGTERM" => crate::posix_signals::SIGTERM,
                _         => 0,
            }
        }

        let cfg = cfg.with_fallback(&[
            ("PANTOMIME_DEFAULT_ACTOR_THROUGHPUT",                                  "10"),
            ("PANTOMIME_DEFAULT_DISPATCHER_LOGIC",                                  "work-stealing"),
            ("PANTOMIME_DEFAULT_DISPATCHER_LOGIC_WORK_STEALING_PARALLELISM_MIN",    "4"),
            ("PANTOMIME_DEFAULT_DISPATCHER_LOGIC_WORK_STEALING_PARALLELISM_MAX",    "64"),
            ("PANTOMIME_DEFAULT_DISPATCHER_LOGIC_WORK_STEALING_PARALLELISM_FACTOR", "1.0"),
            ("PANTOMIME_DEFAULT_DISPATCHER_LOGIC_WORK_STEALING_TASK_QUEUE_FIFO",    "true"),
            ("PANTOMIME_DEFAULT_MAILBOX_LOGIC",                                     "crossbeam-seg-queue"),
            ("PANTOMIME_LOG_CONFIG_ON_START",                                       "false"),
            ("PANTOMIME_MIO_EVENT_CAPACITY",                                        "1024"),
            ("PANTOMIME_MIO_POLL_ERROR_DELAY_MS",                                   "1000"),
            ("PANTOMIME_NUM_CPUS",                                                  "0"),
            ("PANTOMIME_PROCESS_EXIT",                                              "true"),
            ("PANTOMIME_TICKER_INTERVAL_MS",                                        "10"),
            ("PANTOMIME_POSIX_SIGNALS",                                             "SIGINT,SIGTERM,SIGHUP,SIGUSR1,SIGUSR2"),
            ("PANTOMIME_POSIX_EXIT_SIGNALS",                                        "SIGINT,SIGTERM"),
        ]);

        Ok(Self {
            default_actor_throughput:                                   cfg.parsed("PANTOMIME_DEFAULT_ACTOR_THROUGHPUT")?,
            default_dispatcher_logic:                                   cfg.parsed("PANTOMIME_DEFAULT_DISPATCHER_LOGIC")?,
            default_dispatcher_logic_work_stealing_parallelism_min:     cfg.parsed("PANTOMIME_DEFAULT_DISPATCHER_LOGIC_WORK_STEALING_PARALLELISM_MIN")?,
            default_dispatcher_logic_work_stealing_parallelism_max:     cfg.parsed("PANTOMIME_DEFAULT_DISPATCHER_LOGIC_WORK_STEALING_PARALLELISM_MAX")?,
            default_dispatcher_logic_work_stealing_parallelism_factor:  cfg.parsed("PANTOMIME_DEFAULT_DISPATCHER_LOGIC_WORK_STEALING_PARALLELISM_FACTOR")?,
            default_dispatcher_logic_work_stealing_task_queue_fifo:     cfg.parsed("PANTOMIME_DEFAULT_DISPATCHER_LOGIC_WORK_STEALING_TASK_QUEUE_FIFO")?,
            default_mailbox_logic:                                      cfg.parsed("PANTOMIME_DEFAULT_MAILBOX_LOGIC")?,
            log_config_on_start:                                        cfg.parsed("PANTOMIME_LOG_CONFIG_ON_START")?,
            mio_event_capacity:                                         cfg.parsed("PANTOMIME_MIO_EVENT_CAPACITY")?,
            mio_poll_error_delay_ms:                                    cfg.parsed("PANTOMIME_MIO_POLL_ERROR_DELAY_MS")?,
            num_cpus:                                                   cfg.parsed("PANTOMIME_NUM_CPUS")
                                                                           .map(|n| if n == 0 { num_cpus::get() } else { n })?,
            process_exit:                                               cfg.parsed("PANTOMIME_PROCESS_EXIT")?,
            ticker_interval_ms:                                         cfg.parsed("PANTOMIME_TICKER_INTERVAL_MS")?,

            #[cfg(feature = "posix-signals-support")]
            posix_signals:                                              cfg.string_vec("PANTOMIME_POSIX_SIGNALS")?
                                                                           .into_iter()
                                                                           .map(posix_signal)
                                                                           .filter(|s| *s != 0)
                                                                           .collect(),

            #[cfg(feature = "posix-signals-support")]
            posix_shutdown_signals:                                    cfg.string_vec("PANTOMIME_POSIX_EXIT_SIGNALS")?
                                                                          .into_iter()
                                                                          .map(posix_signal)
                                                                          .filter(|s| *s != 0)
                                                                          .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io, time};

    #[derive(Debug, PartialEq)]
    struct CustomConfig {
        latency: u64,
        time: time::Duration,
        words: Vec<String>,
        numbers: Vec<usize>,
    }

    #[test]
    fn test_config() -> io::Result<()> {
        let config = Config::new(&[("LATENCY", "10"), ("TIME", "1")]).with_fallback(&[
            (
                "WORDS",
                "one,two,three\\,with a comma,four with \\\\ a backslash",
            ),
            ("NUMBERS", "4,5,6,77"),
            ("TIME", "2"),
        ]);

        let parsed_config = CustomConfig {
            latency: config.parsed("LATENCY")?,
            time: config.parsed("TIME").map(time::Duration::from_millis)?,
            words: config.string_vec("WORDS")?,
            numbers: config.parsed_vec("NUMBERS")?,
        };

        assert_eq!(
            parsed_config,
            CustomConfig {
                latency: 10,
                time: time::Duration::from_millis(1),
                words: vec![
                    "one".to_string(),
                    "two".to_string(),
                    "three,with a comma".to_string(),
                    "four with \\ a backslash".to_string()
                ],
                numbers: vec![4, 5, 6, 77]
            }
        );

        Ok(())
    }

    #[test]
    fn test_actor_config() {
        assert!(ActorSystemConfig::new(&Config::default()).is_ok());
    }
}
