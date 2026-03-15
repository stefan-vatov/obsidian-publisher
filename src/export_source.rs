use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use walkdir::WalkDir;

/// Strip TOML frontmatter (between +++ fences) from markdown content.
fn strip_frontmatter(content: &str) -> &str {
    let Some(rest) = content.strip_prefix("+++") else {
        return content;
    };
    match rest.find("\n+++") {
        Some(end) => {
            let after = end + 4; // skip past "\n+++"
            rest[after..].trim_start_matches(['\r', '\n'])
        }
        None => content,
    }
}

/// Export markdown files from `source` to `dest`, stripping TOML frontmatter.
pub fn run_export(source: &Path, dest: &Path) -> Result<usize> {
    if !source.is_dir() {
        anyhow::bail!("source directory does not exist: {}", source.display());
    }

    // Clean destination
    if dest.exists() {
        fs::remove_dir_all(dest)
            .with_context(|| format!("failed to clean {}", dest.display()))?;
    }
    fs::create_dir_all(dest)
        .with_context(|| format!("failed to create {}", dest.display()))?;

    let mut count = 0usize;

    for entry in WalkDir::new(source)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();
        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or_default();

        if ext != "md" {
            continue;
        }

        let rel = path
            .strip_prefix(source)
            .with_context(|| format!("failed to get relative path for {}", path.display()))?;

        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;

        let stripped = strip_frontmatter(&content);

        let out_path = dest.join(rel);
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&out_path, stripped)
            .with_context(|| format!("failed to write {}", out_path.display()))?;

        count += 1;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn strip_frontmatter_removes_toml_fences() {
        let input = "+++\ntitle = \"Hello\"\n+++\n\nBody text here.";
        assert_eq!(strip_frontmatter(input), "Body text here.");
    }

    #[test]
    fn strip_frontmatter_preserves_content_without_frontmatter() {
        let input = "Just plain markdown.";
        assert_eq!(strip_frontmatter(input), "Just plain markdown.");
    }

    #[test]
    fn strip_frontmatter_handles_complex_toml() {
        let input = "+++\npath = \"home\"\ntitle = \"Home\"\n\n[extra]\naliases = [\"Start\"]\n\n[taxonomies]\ntags = [\"entry\"]\n+++\n\nWelcome.";
        assert_eq!(strip_frontmatter(input), "Welcome.");
    }

    #[test]
    fn export_copies_and_strips() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("src");
        let dest = tmp.path().join("dest");
        fs::create_dir_all(src.join("nested")).unwrap();

        fs::write(
            src.join("Home.md"),
            "+++\ntitle = \"Home\"\n+++\n\nBody.",
        )
        .unwrap();
        fs::write(
            src.join("nested/Note.md"),
            "+++\ntitle = \"Note\"\n+++\n\nNested body.",
        )
        .unwrap();
        // Non-md file should be skipped
        fs::write(src.join("readme.txt"), "ignore me").unwrap();

        let count = run_export(&src, &dest).unwrap();
        assert_eq!(count, 2);

        let home = fs::read_to_string(dest.join("Home.md")).unwrap();
        assert_eq!(home, "Body.");
        assert!(!home.contains("+++"));

        let note = fs::read_to_string(dest.join("nested/Note.md")).unwrap();
        assert_eq!(note, "Nested body.");

        assert!(!dest.join("readme.txt").exists());
    }
}
