use anyhow::{Context, Result, anyhow};
use clap::{ArgAction, Parser};
use obsidian_publisher::config::RuntimeConfig;
use obsidian_publisher::preprocess::run_publisher;
use obsidian_publisher::watch::run_watch;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(author, version, about = "Obsidian vault to Zola preprocessor")]
struct Cli {
    #[arg(long)]
    vault: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long, default_value_t = false)]
    watch: bool,
    #[arg(short, long, action = ArgAction::Count)]
    verbose: u8,
    #[arg(short, long, default_value_t = false)]
    quiet: bool,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("[error] {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Cli::parse();

    if !args.vault.exists() {
        return Err(anyhow!(
            "vault path does not exist: {}",
            args.vault.display()
        ));
    }

    let app = RuntimeConfig::load(args.config.as_deref())
        .with_context(|| "failed to load preprocessor configuration")?;

    let runtime = RuntimeConfig {
        vault_root: args.vault,
        output_root: args.output,
        app,
        dry_run: args.dry_run,
        watch: args.watch,
        verbose: args.verbose,
        quiet: args.quiet,
    };

    if runtime.watch {
        run_watch(runtime)?;
    } else {
        run_publisher(runtime)?;
    }

    Ok(())
}
