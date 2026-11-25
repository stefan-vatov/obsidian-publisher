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

/// The TOML between the +++ fences, if any.
fn frontmatter_text(content: &str) -> Option<&str> {
    let rest = content.strip_prefix("+++")?;
    rest.find("\n+++").map(|end| &rest[..end])
}

/// Obsidian links such as "[[@handle]]" or "[[Name|Alias]]" shown by name.
fn display_name(author: &str) -> Option<String> {
    let name = author.replace("[[", "").replace("]]", "");
    let name = name.rsplit('|').next().unwrap_or_default().trim().to_string();
    (!name.is_empty()).then_some(name)
}

/// A credit line for a note copied from the web (one with an http(s) `source`),
/// so the exported markdown never travels without its attribution.
fn clipping_credit(frontmatter: &str) -> Option<String> {
    let doc: toml::Value = toml::from_str(frontmatter).ok()?;
    let extra = doc.get("extra")?;
    let source = extra.get("source")?.as_str()?.trim();
    if !(source.starts_with("https://") || source.starts_with("http://")) {
        return None;
    }
    let mut credit = format!("> Clipped from <{source}>");
    if let Some(title) = doc.get("title").and_then(toml::Value::as_str) {
        credit.push_str(&format!(": “{}”", title.trim()));
    }
    let authors: Vec<String> = match extra.get("author") {
        Some(toml::Value::String(one)) => display_name(one).into_iter().collect(),
        Some(toml::Value::Array(many)) => many
            .iter()
            .filter_map(toml::Value::as_str)
            .filter_map(display_name)
            .collect(),
        _ => Vec::new(),
    };
    if !authors.is_empty() {
        credit.push_str(&format!(" by {}", authors.join(", ")));
    }
    if let Some(published) = extra.get("published").and_then(toml::Value::as_str) {
        credit.push_str(&format!(", originally published {}", published.trim()));
    }
    credit.push_str(". All rights stay with the original author.");
    Some(credit)
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
        let credit = frontmatter_text(&content).and_then(clipping_credit);

        let out_path = dest.join(rel);
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Prepend UTF-8 BOM so browsers render correctly even without
        // charset in the Content-Type header (Zola serves text/markdown).
        let mut output = String::with_capacity(3 + stripped.len());
        output.push('\u{FEFF}');
        if let Some(credit) = credit {
            output.push_str(&credit);
            output.push_str("\n\n");
        }
        output.push_str(stripped);
        fs::write(&out_path, output)
            .with_context(|| format!("failed to write {}", out_path.display()))?;

        count += 1;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clipping_credit_names_source_title_authors_and_date() {
        let fm = "\ntitle = \"How I work\"\n\n[extra]\nsource = \"https://x.com/a/status/1\"\nauthor = [\"[[@a]]\", \"[[Bee|B]]\"]\npublished = \"2026-10-25\"\n";
        assert_eq!(
            clipping_credit(fm).as_deref(),
            Some(
                "> Clipped from <https://x.com/a/status/1>: “How I work” by @a, B, originally published 2026-10-25. All rights stay with the original author."
            )
        );
    }

    #[test]
    fn notes_without_a_web_source_get_no_credit() {
        assert_eq!(clipping_credit("title = \"Mine\"\n"), None);
        assert_eq!(
            clipping_credit("[extra]\nsource = \"example.com/page\"\n"),
            None
        );
    }
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
        assert_eq!(home, "\u{FEFF}Body.");
        assert!(!home.contains("+++"));

        let note = fs::read_to_string(dest.join("nested/Note.md")).unwrap();
        assert_eq!(note, "\u{FEFF}Nested body.");

        assert!(!dest.join("readme.txt").exists());
    }
}
