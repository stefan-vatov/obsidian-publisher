use crate::config::RuntimeConfig;
use crate::logging::Reporter;
use crate::preprocess::{run_publisher, run_publisher_incremental};
use anyhow::{Context, Result};
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

pub fn run_watch(runtime: RuntimeConfig) -> Result<()> {
    let reporter = Reporter::new(runtime.verbose, runtime.quiet);
    run_publisher(runtime.clone())?;

    let (tx, rx) = mpsc::channel::<Result<Event, notify::Error>>();

    let mut watcher = RecommendedWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        Config::default(),
    )
    .context("failed to initialize file watcher")?;

    watcher
        .watch(&runtime.vault_root, RecursiveMode::Recursive)
        .with_context(|| format!("failed to watch {}", runtime.vault_root.display()))?;

    reporter.info("watch mode active");

    let debounce = Duration::from_millis(runtime.app.watch.debounce_ms.max(50));
    let mut last_run = Instant::now();

    loop {
        let event = rx.recv().context("watch channel closed")?;
        match event {
            Ok(evt) => {
                let elapsed = last_run.elapsed();
                if elapsed < debounce {
                    thread::sleep(debounce - elapsed);
                }

                let changed_paths = collect_event_paths(&runtime, evt, &rx, &reporter);
                if changed_paths.is_empty() {
                    last_run = Instant::now();
                    continue;
                }

                match run_publisher_incremental(runtime.clone(), &changed_paths) {
                    Ok(_) => {
                        last_run = Instant::now();
                    }
                    Err(err) => {
                        reporter.error(format!("watch rebuild failed: {err:#}"));
                        last_run = Instant::now();
                    }
                }
            }
            Err(err) => {
                reporter.warn(format!("watcher error: {err}"));
            }
        }
    }
}

fn collect_event_paths(
    runtime: &RuntimeConfig,
    initial: Event,
    rx: &mpsc::Receiver<Result<Event, notify::Error>>,
    reporter: &Reporter,
) -> Vec<PathBuf> {
    let mut changed = BTreeSet::new();
    merge_event_paths(runtime, &initial, &mut changed);

    // Drain burst events before re-running to reduce duplicate incremental rebuilds.
    while let Ok(next) = rx.recv_timeout(Duration::from_millis(35)) {
        match next {
            Ok(evt) => merge_event_paths(runtime, &evt, &mut changed),
            Err(err) => reporter.warn(format!("watcher error: {err}")),
        }
    }

    changed.into_iter().collect()
}

fn merge_event_paths(runtime: &RuntimeConfig, event: &Event, changed: &mut BTreeSet<PathBuf>) {
    for path in &event.paths {
        if let Some(normalized) = normalize_watch_path(runtime, path) {
            changed.insert(normalized);
        }
    }
}

fn normalize_watch_path(runtime: &RuntimeConfig, path: &Path) -> Option<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        runtime.vault_root.join(path)
    };

    if absolute.starts_with(&runtime.output_root) {
        return None;
    }
    if !absolute.starts_with(&runtime.vault_root) {
        return None;
    }

    Some(absolute)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;

    fn runtime(vault: &Path, output: &Path) -> RuntimeConfig {
        RuntimeConfig {
            vault_root: vault.to_path_buf(),
            output_root: output.to_path_buf(),
            app: AppConfig::default(),
            dry_run: false,
            watch: true,
            verbose: 0,
            quiet: true,
        }
    }

    #[test]
    fn normalize_watch_path_keeps_vault_paths() {
        let runtime = runtime(Path::new("/tmp/vault"), Path::new("/tmp/site"));
        let path = Path::new("/tmp/vault/notes/A.md");
        assert_eq!(
            normalize_watch_path(&runtime, path),
            Some(path.to_path_buf())
        );
    }

    #[test]
    fn normalize_watch_path_rejects_output_and_external_paths() {
        let runtime = runtime(Path::new("/tmp/vault"), Path::new("/tmp/vault/site"));
        assert!(
            normalize_watch_path(&runtime, Path::new("/tmp/vault/site/content/A.md")).is_none()
        );
        assert!(normalize_watch_path(&runtime, Path::new("/tmp/other/A.md")).is_none());
    }
}
