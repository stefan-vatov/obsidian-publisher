# obsidian-publisher

Rust CLI that preprocesses Obsidian vault markdown into Zola-ready content.

## Features

- Vault scan with ignore globs
- `publish: true` filter
- Frontmatter normalization to Zola TOML/YAML
- Wikilink conversion with missing-link fallback styling
- Embed expansion with depth limit
- Obsidian callout transformation
- Backlinks + `graph.json` emission
- Asset copy + asset link rewriting
- `--dry-run` and `--watch`

## Usage

```bash
cargo run -- --vault ../vault --output .. --config ../preprocessor.toml
```

Or build release binary:

```bash
cargo build --release
./target/release/obsidian-publisher --vault ../vault --output .. --config ../preprocessor.toml
```

## Tests

```bash
cargo test
```
