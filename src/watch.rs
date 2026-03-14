use crate::config::RuntimeConfig;
use crate::logging::Reporter;
use crate::preprocess::{run_publisher, run_publisher_incremental};
use anyhow::{Context, Result};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
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

                let batch = collect_event_batch(&runtime, evt, &rx, &reporter);

                if batch.has_directory_events {
                    // Directory-level events (rename, delete) may not enumerate
                    // individual child files on all platforms. Fall back to a
                    // full rebuild to avoid leaving stale outputs behind.
                    reporter.debug("directory event detected; performing full rebuild");
                    match run_publisher(runtime.clone()) {
                        Ok(_) => {}
                        Err(err) => reporter.error(format!("watch rebuild failed: {err:#}")),
                    }
                    last_run = Instant::now();
                    continue;
                }

                if batch.changed_paths.is_empty() {
                    last_run = Instant::now();
                    continue;
                }

                match run_publisher_incremental(runtime.clone(), &batch.changed_paths) {
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

struct EventBatch {
    changed_paths: Vec<PathBuf>,
    has_directory_events: bool,
}

fn collect_event_batch(
    runtime: &RuntimeConfig,
    initial: Event,
    rx: &mpsc::Receiver<Result<Event, notify::Error>>,
    reporter: &Reporter,
) -> EventBatch {
    let mut changed = BTreeSet::new();
    let mut has_directory_events = false;

    merge_event_paths(runtime, &initial, &mut changed, &mut has_directory_events);

    // Drain burst events before re-running to reduce duplicate incremental rebuilds.
    while let Ok(next) = rx.recv_timeout(Duration::from_millis(35)) {
        match next {
            Ok(evt) => merge_event_paths(runtime, &evt, &mut changed, &mut has_directory_events),
            Err(err) => reporter.warn(format!("watcher error: {err}")),
        }
    }

    EventBatch {
        changed_paths: changed.into_iter().collect(),
        has_directory_events,
    }
}

fn merge_event_paths(
    runtime: &RuntimeConfig,
    event: &Event,
    changed: &mut BTreeSet<PathBuf>,
    has_directory_events: &mut bool,
) {
    // Detect directory-level events from the event kind rather than filesystem
    // state. A deleted/renamed directory no longer exists on disk, so
    // path.is_dir() would return false and miss it. Remove and rename events
    // that affect paths without file extensions are conservatively treated as
    // directory events to trigger a full rebuild.
    let is_remove_or_rename = matches!(
        event.kind,
        EventKind::Remove(_) | EventKind::Modify(notify::event::ModifyKind::Name(_))
    );

    for path in &event.paths {
        let Some(normalized) = normalize_watch_path(runtime, path) else {
            continue;
        };

        // If the path currently is a directory, or if it was removed/renamed
        // and has no file extension (likely a directory), flag for full rebuild.
        if path.is_dir()
            || (is_remove_or_rename && path.extension().is_none())
        {
            *has_directory_events = true;
            continue;
        }

        changed.insert(normalized);
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
