use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub publish: PublishConfig,
    pub ignore_globs: Vec<String>,
    pub frontmatter: FrontmatterConfig,
    pub graph: GraphConfig,
    pub dataview: DataviewConfig,
    pub embeds: EmbedConfig,
    pub markdown: MarkdownConfig,
    pub watch: WatchConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            publish: PublishConfig::default(),
            ignore_globs: vec![
                ".obsidian/**".to_string(),
                ".trash/**".to_string(),
                "**/.DS_Store".to_string(),
            ],
            frontmatter: FrontmatterConfig::default(),
            graph: GraphConfig::default(),
            dataview: DataviewConfig::default(),
            embeds: EmbedConfig::default(),
            markdown: MarkdownConfig::default(),
            watch: WatchConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PublishConfig {
    pub require_explicit: bool,
}

impl Default for PublishConfig {
    fn default() -> Self {
        Self {
            require_explicit: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FrontmatterConfig {
    pub output: FrontmatterOutput,
}

impl Default for FrontmatterConfig {
    fn default() -> Self {
        Self {
            output: FrontmatterOutput::Toml,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FrontmatterOutput {
    Toml,
    Yaml,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct GraphConfig {
    pub include_tag_nodes: bool,
    pub enabled: bool,
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            include_tag_nodes: false,
            enabled: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DataviewConfig {
    pub mode: DataviewMode,
    /// Backward-compatible override for older configs.
    /// When set and `mode` is left at default `strip`, this toggles strip/preserve.
    pub strip_inline_fields: Option<bool>,
}

impl DataviewConfig {
    pub fn effective_mode(&self) -> DataviewMode {
        match (self.mode, self.strip_inline_fields) {
            (DataviewMode::Strip, Some(false)) => DataviewMode::Preserve,
            (mode, _) => mode,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataviewMode {
    Strip,
    Preserve,
    PreserveToFrontmatter,
}

impl Default for DataviewMode {
    fn default() -> Self {
        Self::Strip
    }
}

impl Default for DataviewConfig {
    fn default() -> Self {
        Self {
            mode: DataviewMode::Strip,
            strip_inline_fields: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct EmbedConfig {
    pub max_depth: usize,
}

impl Default for EmbedConfig {
    fn default() -> Self {
        Self { max_depth: 3 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MarkdownConfig {
    pub strip_comments: bool,
    pub convert_highlights: bool,
    pub soft_line_breaks: SoftLineBreakMode,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SoftLineBreakMode {
    Preserve,
    HardBreaks,
}

impl Default for SoftLineBreakMode {
    fn default() -> Self {
        Self::Preserve
    }
}

impl Default for MarkdownConfig {
    fn default() -> Self {
        Self {
            strip_comments: true,
            convert_highlights: true,
            soft_line_breaks: SoftLineBreakMode::Preserve,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WatchConfig {
    pub debounce_ms: u64,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self { debounce_ms: 350 }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub vault_root: PathBuf,
    pub output_root: PathBuf,
    pub app: AppConfig,
    pub dry_run: bool,
    pub watch: bool,
    pub verbose: u8,
    pub quiet: bool,
}

impl RuntimeConfig {
    pub fn load(config_path: Option<&Path>) -> Result<AppConfig> {
        let Some(path) = config_path else {
            return Ok(AppConfig::default());
        };

        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let cfg: AppConfig =
            toml::from_str(&raw).with_context(|| format!("invalid TOML in {}", path.display()))?;
        Ok(cfg)
    }
}

/// Resolve a path by canonicalizing its deepest existing ancestor, then
/// appending the remaining (not-yet-created) tail components. This catches
/// symlinked parents even when the final directory does not exist yet.
pub fn resolve_through_existing_ancestors(path: &Path) -> PathBuf {
    if let Ok(canonical) = path.canonicalize() {
        return canonical;
    }

    let mut tail_components = Vec::new();
    let mut current = path.to_path_buf();
    loop {
        if let Ok(canonical) = current.canonicalize() {
            let mut resolved = canonical;
            for component in tail_components.into_iter().rev() {
                resolved.push(component);
            }
            return resolved;
        }
        if let Some(file_name) = current.file_name() {
            tail_components.push(file_name.to_os_string());
        } else {
            break;
        }
        if !current.pop() {
            break;
        }
    }

    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataview_mode_back_compat_strip_inline_fields_false_maps_to_preserve() {
        let cfg: AppConfig =
            toml::from_str("[dataview]\nstrip_inline_fields = false\n").expect("valid toml");
        assert_eq!(cfg.dataview.mode, DataviewMode::Strip);
        assert_eq!(cfg.dataview.effective_mode(), DataviewMode::Preserve);
    }

    #[test]
    fn dataview_mode_preserve_to_frontmatter_wins_over_legacy_flag() {
        let cfg: AppConfig = toml::from_str(
            "[dataview]\nmode = \"preserve_to_frontmatter\"\nstrip_inline_fields = true\n",
        )
        .expect("valid toml");
        assert_eq!(
            cfg.dataview.effective_mode(),
            DataviewMode::PreserveToFrontmatter
        );
    }

    #[test]
    fn markdown_soft_line_break_mode_deserializes() {
        let cfg: AppConfig =
            toml::from_str("[markdown]\nsoft_line_breaks = \"hard_breaks\"\n").expect("valid toml");
        assert_eq!(cfg.markdown.soft_line_breaks, SoftLineBreakMode::HardBreaks);
    }
}
