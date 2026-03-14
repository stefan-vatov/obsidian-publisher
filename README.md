# obsidian-publisher

Rust CLI that preprocesses an [Obsidian](https://obsidian.md/) vault into [Zola](https://www.getzola.org/)-ready content.

## Features

- Vault scan with configurable ignore globs
- `publish: true` frontmatter filter (opt-in or opt-out)
- Frontmatter normalization to Zola-compatible TOML (or YAML)
- Wikilink → standard markdown link conversion with disambiguation
- Embed/transclusion expansion with depth limit and cycle detection
- Obsidian callout → HTML transformation
- Backlink tracking and `graph.json` emission for link graph visualization
- Asset copy and link rewriting
- Dataview inline field stripping, preservation, or extraction to frontmatter
- Highlight (`==text==`) → `<mark>` conversion
- Incremental rebuilds in `--watch` mode
- `--dry-run` to preview changes without writing

## Install

```bash
cargo install obsidian-publisher
```

Or build from source:

```bash
git clone https://github.com/stefan-vatov/obsidian-publisher.git
cd obsidian-publisher
cargo install --path .
```

## Usage

```bash
obsidian-publisher --vault ./vault --output ./site --config preprocessor.toml
```

### Flags

| Flag | Description |
|------|-------------|
| `--vault <PATH>` | Path to the Obsidian vault directory (required) |
| `--output <PATH>` | Path to the output directory, e.g. your Zola site root (required) |
| `--config <PATH>` | Path to a TOML configuration file (optional, uses defaults if omitted) |
| `--dry-run` | Preview what would be written without touching the filesystem |
| `--watch` | Run continuously, rebuilding incrementally on vault changes |
| `-v` | Debug output (scan details, incremental plans) |
| `-vv` | Trace output (per-file decisions, symlink skips) |
| `-q` / `--quiet` | Suppress all output except errors |

### Important: output directory placement

The `--output` directory **must not** be inside the `--vault` directory. If it is, generated files would be re-ingested as source on subsequent runs, causing recursive corruption. The CLI will reject this configuration with an error.

## Output structure

Given `--output ./site`, the publisher writes:

```
site/
  content/          # Zola-compatible markdown (one file per published note)
    _index.md       # Auto-generated section indexes
    ...
  static/
    assets/         # Copied vault assets (images, PDFs, etc.)
    graph.json      # Link graph data (nodes + edges)
```

On a full run, `content/`, `static/assets/`, and `static/graph.json` are **deleted and regenerated**. Do not place hand-edited files in these directories.

## Configuration

Create a TOML file (e.g. `preprocessor.toml`) and pass it via `--config`. All sections are optional — defaults are shown below.

```toml
ignore_globs = [
  ".obsidian/**",
  ".trash/**",
  "**/.DS_Store",
]

[publish]
require_explicit = true          # Only publish notes with `publish: true` in frontmatter

[frontmatter]
output = "toml"                  # "toml" (default) or "yaml"

[graph]
enabled = true
include_tag_nodes = false        # Add tag nodes to graph.json

[dataview]
# "strip" (default) — remove inline fields
# "preserve" — keep inline fields as-is
# "preserve_to_frontmatter" — extract fields into Zola frontmatter [extra]
mode = "strip"

[embeds]
max_depth = 3                    # Max transclusion nesting depth

[markdown]
strip_comments = true            # Remove %%obsidian comments%%
convert_highlights = true        # ==text== → <mark>text</mark>
soft_line_breaks = "preserve"    # "preserve" (default) or "hard_breaks"

[watch]
debounce_ms = 350                # Debounce interval for file watcher
```

## Tests

```bash
cargo test
```

## Releases

Merges to `main` trigger two GitHub Actions workflows:

1. **Bump** — Cocogitto computes the next version from Conventional Commits, updates `Cargo.toml`, `CHANGELOG.md`, commits, and pushes a `v*.*.*` tag
2. **Release** — triggered by the new tag, builds a Linux `x86_64-unknown-linux-musl` binary and publishes the archive and checksum to GitHub Releases

The published release asset is named like:

```text
obsidian-publisher-0.2.0-x86_64-unknown-linux-musl.tar.gz
```

Release versioning is driven by Conventional Commits via [`cog`](https://docs.cocogitto.io/). `feat` produces a minor release, breaking changes produce a major release, and the remaining standard commit types are configured to publish patch releases.

The automated release build uses the repository toolchain pin, currently Rust 1.88.

## License

MIT
