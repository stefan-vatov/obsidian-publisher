#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Quiet,
    Info,
    Debug,
}

#[derive(Debug, Clone, Copy)]
pub struct Reporter {
    level: Level,
}

impl Reporter {
    pub fn new(verbose: u8, quiet: bool) -> Self {
        let level = if quiet {
            Level::Quiet
        } else if verbose > 0 {
            Level::Debug
        } else {
            Level::Info
        };
        Self { level }
    }

    pub fn info(&self, message: impl AsRef<str>) {
        if self.level >= Level::Info {
            eprintln!("[info] {}", message.as_ref());
        }
    }

    pub fn debug(&self, message: impl AsRef<str>) {
        if self.level >= Level::Debug {
            eprintln!("[debug] {}", message.as_ref());
        }
    }

    pub fn warn(&self, message: impl AsRef<str>) {
        if self.level >= Level::Info {
            eprintln!("[warn] {}", message.as_ref());
        }
    }

    pub fn error(&self, message: impl AsRef<str>) {
        eprintln!("[error] {}", message.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quiet_overrides_verbose() {
        let reporter = Reporter::new(2, true);
        assert_eq!(reporter.level, Level::Quiet);
    }

    #[test]
    fn verbose_enables_debug_level() {
        let reporter = Reporter::new(1, false);
        assert_eq!(reporter.level, Level::Debug);
    }

    #[test]
    fn default_level_is_info() {
        let reporter = Reporter::new(0, false);
        assert_eq!(reporter.level, Level::Info);
    }
}
