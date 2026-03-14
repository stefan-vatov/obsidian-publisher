use serde::Serialize;
use serde_yaml::Mapping;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct RawNote {
    pub id: usize,
    pub source_path: PathBuf,
    pub relative_path: PathBuf,
    pub stem: String,
    pub url_path: String,
    pub frontmatter: Mapping,
    pub body: String,
    pub metadata: ParsedMetadata,
    pub published: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ParsedMetadata {
    pub title: Option<String>,
    pub tags: Vec<String>,
    pub aliases: Vec<String>,
    pub css_classes: Vec<String>,
    pub date: Option<String>,
    pub updated: Option<String>,
    pub extra: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone)]
pub struct ProcessedNote {
    pub note_id: usize,
    pub output_relative_path: PathBuf,
    pub content: String,
    pub outbound_ids: HashSet<usize>,
    pub metadata: ParsedMetadata,
}

#[derive(Debug, Clone)]
pub struct AssetFile {
    pub source_path: PathBuf,
    pub relative_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphNode {
    pub id: String,
    pub title: String,
    pub path: String,
    pub tags: Vec<String>,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GraphData {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacklinkItem {
    pub title: String,
    pub path: String,
}

#[derive(Debug, Clone, Default)]
pub struct RunSummary {
    pub notes_total: usize,
    pub notes_published: usize,
    pub assets_total: usize,
    pub errors: usize,
}
