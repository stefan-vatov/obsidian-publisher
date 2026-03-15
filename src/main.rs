use anyhow::{Context, Result, anyhow};
use clap::{ArgAction, Parser, Subcommand};
use obsidian_publisher::config::{RuntimeConfig, resolve_through_existing_ancestors};
use obsidian_publisher::export_source::run_export;
use obsidian_publisher::preprocess::run_publisher;
use obsidian_publisher::watch::run_watch;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(author, version, about = "Obsidian vault to Zola preprocessor")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    // Top-level args for backward compatibility (publish mode)
    #[arg(long)]
    vault: Option<PathBuf>,
    #[arg(long)]
    output: Option<PathBuf>,
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

#[derive(Debug, Subcommand)]
enum Commands {
    /// Export markdown source files with frontmatter stripped
    ExportSource {
        /// Source directory (e.g. site/content)
        #[arg(long)]
        source: PathBuf,
        /// Destination directory (e.g. site/public/_source)
        #[arg(long)]
        dest: PathBuf,
    },
}

fn main() {
    if let Err(err) = run() {
        eprintln!("[error] {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Cli::parse();

    // Subcommand dispatch
    if let Some(cmd) = args.command {
        return match cmd {
            Commands::ExportSource { source, dest } => {
                let count = run_export(&source, &dest)?;
                eprintln!("[export-source] exported {count} file(s)");
                Ok(())
            }
        };
    }

    // Default: publish mode (backward compatible)
    let vault_arg = args.vault.ok_or_else(|| anyhow!("--vault is required"))?;
    let output_arg = args.output.ok_or_else(|| anyhow!("--output is required"))?;

    let vault = vault_arg.canonicalize().unwrap_or_else(|_| vault_arg.clone());

    if !vault.exists() {
        return Err(anyhow!(
            "vault path does not exist: {}",
            vault.display()
        ));
    }

    let output = resolve_through_existing_ancestors(&output_arg);

    if output.starts_with(&vault) {
        return Err(anyhow!(
            "output directory ({}) must not be inside the vault ({}); \
             this would cause generated files to be re-ingested on subsequent runs",
            output.display(),
            vault.display()
        ));
    }

    let app = RuntimeConfig::load(args.config.as_deref())
        .with_context(|| "failed to load preprocessor configuration")?;

    let runtime = RuntimeConfig {
        vault_root: vault,
        output_root: output,
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
