use crate::config::{DataviewMode, FrontmatterOutput, RuntimeConfig, SoftLineBreakMode};
use crate::logging::Reporter;
use crate::markdown::{
    convert_highlights, convert_soft_breaks_to_hard, extract_dataview_inline_fields,
    extract_heading_section, html_escape, is_external_or_anchor, markdown_link_target,
    normalize_note_target, normalize_relative_link, parse_wikilink_spec, slugify,
    split_frontmatter, strip_dataview_inline_fields, strip_obsidian_comments, transform_callouts,
    url_path_from_relative,
};
use crate::model::{
    AssetFile, BacklinkItem, GraphData, GraphEdge, GraphNode, ParsedMetadata, ProcessedNote,
    RawNote, RunSummary,
};
use anyhow::{Context, Result, anyhow};
use globset::{Glob, GlobSet, GlobSetBuilder};
use regex::{Captures, Regex};
use serde_yaml::{Mapping, Value as YamlValue};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

#[derive(Debug, Clone)]
struct NoteLookup {
    by_stem: HashMap<String, Vec<usize>>,
    by_path: HashMap<String, usize>,
}

#[derive(Debug, Clone)]
struct AssetLookup {
    paths: HashSet<PathBuf>,
    by_basename: HashMap<String, Vec<PathBuf>>,
}

#[derive(Debug, Clone)]
enum AssetResolution {
    Resolved(PathBuf),
    Missing,
    Ambiguous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode {
    Full,
    Incremental,
}

#[derive(Debug, Clone, Default)]
struct NoteDependencyScan {
    backlink_targets: HashMap<usize, HashSet<usize>>,
    note_targets: HashMap<usize, HashSet<usize>>,
    embed_targets: HashMap<usize, HashSet<usize>>,
    unresolved_note_targets: HashMap<usize, HashSet<String>>,
    resolved_asset_targets: HashMap<usize, HashSet<PathBuf>>,
    unresolved_asset_targets: HashMap<usize, HashSet<String>>,
}

#[derive(Debug, Clone, Default)]
struct IncrementalPlan {
    note_ids: Vec<usize>,
    asset_relative_paths: Vec<PathBuf>,
}

pub struct Publisher {
    runtime: RuntimeConfig,
    reporter: Reporter,
    ignore_matcher: GlobSet,
}

impl Publisher {
    pub fn new(runtime: RuntimeConfig, reporter: Reporter) -> Result<Self> {
        // Validate that output is not inside the vault (library-level guard).
        // Use resolve_through_existing_ancestors so that non-existent output
        // paths with symlinked parents are still caught.
        let vault_resolved = crate::config::resolve_through_existing_ancestors(&runtime.vault_root);
        let output_resolved =
            crate::config::resolve_through_existing_ancestors(&runtime.output_root);
        if output_resolved.starts_with(&vault_resolved) {
            return Err(anyhow!(
                "output directory ({}) must not be inside the vault ({})",
                runtime.output_root.display(),
                runtime.vault_root.display()
            ));
        }

        let ignore_matcher = build_ignore_matcher(&runtime.app.ignore_globs)?;
        Ok(Self {
            runtime,
            reporter,
            ignore_matcher,
        })
    }

    pub fn run_once(&self) -> Result<RunSummary> {
        self.run_internal(RunMode::Full, &HashSet::new())
    }

    pub fn run_incremental(&self, changed_paths: &[PathBuf]) -> Result<RunSummary> {
        let changed_relative_paths = self.collect_changed_relative_paths(changed_paths);
        if changed_relative_paths.is_empty() {
            self.reporter
                .debug("no relevant changed paths detected; skipping incremental publish");
            return Ok(RunSummary::default());
        }
        self.run_internal(RunMode::Incremental, &changed_relative_paths)
    }

    fn run_internal(
        &self,
        mode: RunMode,
        changed_relative_paths: &HashSet<PathBuf>,
    ) -> Result<RunSummary> {
        self.reporter.info(format!(
            "processing vault {}",
            self.runtime.vault_root.display()
        ));

        if mode == RunMode::Incremental {
            self.reporter.debug(format!(
                "incremental rebuild with {} changed path(s)",
                changed_relative_paths.len()
            ));
        }

        let (notes, assets, scan_errors) = self.scan_vault()?;
        let lookup = build_lookup(&notes);
        let asset_lookup = build_asset_lookup(&assets);
        let published_ids: HashSet<usize> =
            notes.iter().filter(|n| n.published).map(|n| n.id).collect();
        let dependencies = self.collect_note_dependencies(&notes, &lookup, &asset_lookup);
        let (graph, backlinks) =
            self.build_graph_and_backlinks_from_dependencies(&notes, &dependencies, &published_ids);

        self.reporter.debug(format!(
            "scanned {} markdown files and {} assets",
            notes.len(),
            assets.len()
        ));

        let (note_ids_to_process, asset_relative_paths) = match mode {
            RunMode::Full => {
                let mut note_ids = notes
                    .iter()
                    .filter(|note| note.published)
                    .map(|note| note.id)
                    .collect::<Vec<_>>();
                note_ids.sort_unstable();

                let mut asset_paths = assets
                    .iter()
                    .map(|asset| asset.relative_path.clone())
                    .collect::<Vec<_>>();
                asset_paths.sort_by_key(|path| path_to_unix(path));
                (note_ids, asset_paths)
            }
            RunMode::Incremental => {
                let plan = self.build_incremental_plan(
                    changed_relative_paths,
                    &notes,
                    &assets,
                    &dependencies,
                );
                self.reporter.debug(format!(
                    "incremental plan: {} note(s), {} asset(s)",
                    plan.note_ids.len(),
                    plan.asset_relative_paths.len()
                ));
                (plan.note_ids, plan.asset_relative_paths)
            }
        };

        let mut processed = Vec::new();
        let mut failures = scan_errors.len();
        for err in scan_errors {
            self.reporter.error(err);
        }

        for note_id in note_ids_to_process {
            let note = &notes[note_id];
            match self.process_note(note, &notes, &lookup, &asset_lookup) {
                Ok(p) => processed.push(p),
                Err(err) => {
                    failures += 1;
                    self.reporter.error(format!(
                        "failed to process {}: {err:#}",
                        note.relative_path.display()
                    ));
                }
            }
        }

        if !self.runtime.dry_run {
            match mode {
                RunMode::Full => self.prepare_output_dirs()?,
                RunMode::Incremental => {
                    self.prepare_output_dirs_incremental()?;
                    self.remove_stale_changed_outputs(changed_relative_paths)?;
                }
            }
            match mode {
                RunMode::Full => self.write_assets(&assets)?,
                RunMode::Incremental => {
                    self.write_assets_incremental(&assets, &asset_relative_paths)?
                }
            }
            self.write_notes(&notes, processed.as_slice(), &backlinks)?;
            if mode == RunMode::Full {
                self.write_section_indexes(processed.as_slice())?;
            } else {
                // On incremental runs, compute section dirs from ALL published
                // notes (not just the processed subset), then prune any stale
                // _index.md files left behind after the last note in a folder
                // was deleted/unpublished.
                let all_published: Vec<ProcessedNote> = notes
                    .iter()
                    .filter(|n| n.published)
                    .map(|n| ProcessedNote {
                        note_id: n.id,
                        output_relative_path: n.relative_path.clone(),
                        content: String::new(),
                        outbound_ids: HashSet::new(),
                        metadata: n.metadata.clone(),
                    })
                    .collect();
                self.write_section_indexes(&all_published)?;
                self.prune_empty_section_indexes(&all_published)?;
            }
            if self.runtime.app.graph.enabled {
                self.write_graph(&graph)?;
            }
        } else {
            if mode == RunMode::Incremental {
                for line in self.build_dry_run_removal_lines(changed_relative_paths) {
                    self.reporter.info(line);
                }
            }
            for line in self.build_dry_run_report_lines(&processed, &asset_relative_paths) {
                self.reporter.info(line);
            }
            self.reporter.info("dry-run enabled; no files written");
        }

        let summary = RunSummary {
            notes_total: notes.len(),
            notes_published: processed.len(),
            assets_total: asset_relative_paths.len(),
            errors: failures,
        };

        if failures > 0 {
            return Err(anyhow!(
                "processing completed with {} file errors",
                failures
            ));
        }

        self.reporter.info(format!(
            "done: {} published notes, {} assets",
            summary.notes_published, summary.assets_total
        ));

        Ok(summary)
    }

    fn collect_changed_relative_paths(&self, changed_paths: &[PathBuf]) -> HashSet<PathBuf> {
        changed_paths
            .iter()
            .filter_map(|changed| {
                let absolute = if changed.is_absolute() {
                    changed.clone()
                } else {
                    self.runtime.vault_root.join(changed)
                };

                if absolute.starts_with(&self.runtime.output_root) {
                    return None;
                }

                let relative = absolute
                    .strip_prefix(&self.runtime.vault_root)
                    .ok()?
                    .to_path_buf();

                if self.is_ignored_rel(&relative) {
                    return None;
                }

                Some(relative)
            })
            .collect()
    }

    fn collect_note_dependencies(
        &self,
        notes: &[RawNote],
        lookup: &NoteLookup,
        assets: &AssetLookup,
    ) -> NoteDependencyScan {
        let wiki_re = Regex::new(r"\[\[([^\]]+)\]\]").expect("valid regex");
        let image_re = Regex::new(r"!\[[^\]]*\]\(([^)]+)\)").expect("valid regex");
        let link_re = Regex::new(r"\[[^\]]+\]\(([^)]+)\)").expect("valid regex");

        let mut scan = NoteDependencyScan::default();

        for note in notes.iter().filter(|note| note.published) {
            let mut backlink_targets = HashSet::new();
            let mut note_targets = HashSet::new();
            let mut embed_targets = HashSet::new();
            let mut unresolved_note_targets = HashSet::new();
            let mut resolved_asset_targets = HashSet::new();
            let mut unresolved_asset_targets = HashSet::new();
            let bytes = note.body.as_bytes();

            for caps in wiki_re.captures_iter(&note.body) {
                let m = caps.get(0).expect("match exists");
                let spec_raw = caps.get(1).map(|m| m.as_str()).unwrap_or_default();
                let spec = parse_wikilink_spec(spec_raw);
                let target = normalize_note_target(&spec.target);
                if target.is_empty() {
                    continue;
                }

                let is_embed = m.start() > 0 && bytes[m.start() - 1] == b'!';
                if is_probably_asset(&target) {
                    self.collect_asset_reference_tokens(
                        note,
                        &target,
                        assets,
                        &mut resolved_asset_targets,
                        &mut unresolved_asset_targets,
                    );
                    continue;
                }

                if let Some(target_id) = self.resolve_note_target(note.id, &target, notes, lookup) {
                    note_targets.insert(target_id);
                    if is_embed {
                        embed_targets.insert(target_id);
                    } else {
                        backlink_targets.insert(target_id);
                    }
                } else {
                    unresolved_note_targets.extend(note_target_tokens(&target));
                }
            }

            for caps in image_re.captures_iter(&note.body) {
                let target_raw = caps.get(1).map(|m| m.as_str()).unwrap_or_default();
                let target = markdown_link_target(target_raw);
                if is_external_or_anchor(target.as_ref()) {
                    continue;
                }
                let (target_path, _) = split_link_fragment(target.as_ref());
                self.collect_asset_reference_tokens(
                    note,
                    target_path,
                    assets,
                    &mut resolved_asset_targets,
                    &mut unresolved_asset_targets,
                );
            }

            for caps in link_re.captures_iter(&note.body) {
                let Some(entire) = caps.get(0).map(|m| m.as_str()) else {
                    continue;
                };
                if entire.starts_with("![") {
                    continue;
                }

                let target_raw = caps.get(1).map(|m| m.as_str()).unwrap_or_default();
                let target = markdown_link_target(target_raw);
                if is_external_or_anchor(target.as_ref()) {
                    continue;
                }

                let (target_path, _) = split_link_fragment(target.as_ref());
                if target_path.to_ascii_lowercase().ends_with(".md") {
                    let normalized = normalize_note_target(target_path);
                    if let Some(target_id) =
                        self.resolve_note_target(note.id, &normalized, notes, lookup)
                    {
                        note_targets.insert(target_id);
                    } else {
                        unresolved_note_targets.extend(note_target_tokens(&normalized));
                    }
                } else {
                    self.collect_asset_reference_tokens(
                        note,
                        target_path,
                        assets,
                        &mut resolved_asset_targets,
                        &mut unresolved_asset_targets,
                    );
                }
            }

            scan.backlink_targets.insert(note.id, backlink_targets);
            scan.note_targets.insert(note.id, note_targets);
            scan.embed_targets.insert(note.id, embed_targets);
            scan.unresolved_note_targets
                .insert(note.id, unresolved_note_targets);
            scan.resolved_asset_targets
                .insert(note.id, resolved_asset_targets);
            scan.unresolved_asset_targets
                .insert(note.id, unresolved_asset_targets);
        }

        scan
    }

    fn collect_asset_reference_tokens(
        &self,
        note: &RawNote,
        target: &str,
        assets: &AssetLookup,
        resolved_asset_targets: &mut HashSet<PathBuf>,
        unresolved_asset_targets: &mut HashSet<String>,
    ) {
        match self.resolve_asset_reference(&note.relative_path, target, assets) {
            AssetResolution::Resolved(path) => {
                resolved_asset_targets.insert(path);
            }
            AssetResolution::Ambiguous | AssetResolution::Missing => {
                unresolved_asset_targets.extend(asset_target_tokens(target));
            }
        }
    }

    fn build_incremental_plan(
        &self,
        changed_relative_paths: &HashSet<PathBuf>,
        notes: &[RawNote],
        assets: &[AssetFile],
        dependencies: &NoteDependencyScan,
    ) -> IncrementalPlan {
        let mut changed_note_paths = HashSet::new();
        let mut changed_asset_paths = HashSet::new();
        for rel in changed_relative_paths {
            let ext = rel
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            if ext == "md" {
                changed_note_paths.insert(rel.clone());
            } else {
                changed_asset_paths.insert(rel.clone());
            }
        }

        let mut changed_note_ids = HashSet::new();
        for note in notes {
            if changed_note_paths.contains(&note.relative_path) {
                changed_note_ids.insert(note.id);
            }
        }

        let reverse_note_targets = reverse_note_dependencies(&dependencies.note_targets);
        let reverse_embed_targets = reverse_note_dependencies(&dependencies.embed_targets);

        let mut impacted = HashSet::new();

        for note_id in &changed_note_ids {
            if notes[*note_id].published {
                impacted.insert(*note_id);
            }
        }

        for target_id in &changed_note_ids {
            if let Some(sources) = reverse_note_targets.get(target_id) {
                for source_id in sources {
                    if notes[*source_id].published {
                        impacted.insert(*source_id);
                    }
                }
            }
        }

        for source_id in &changed_note_ids {
            if !notes[*source_id].published {
                continue;
            }
            if let Some(targets) = dependencies.backlink_targets.get(source_id) {
                for target_id in targets {
                    if notes[*target_id].published {
                        impacted.insert(*target_id);
                    }
                }
            }
        }

        let mut changed_note_target_tokens = HashSet::new();
        let mut changed_note_urls = HashSet::new();
        for rel in &changed_note_paths {
            changed_note_target_tokens.extend(note_target_tokens_for_relative_path(rel));
            changed_note_urls.insert(url_path_from_relative(rel));
        }

        if !changed_note_target_tokens.is_empty() {
            for note in notes.iter().filter(|note| note.published) {
                if dependencies
                    .unresolved_note_targets
                    .get(&note.id)
                    .is_some_and(|targets| {
                        targets
                            .iter()
                            .any(|target| changed_note_target_tokens.contains(target))
                    })
                {
                    impacted.insert(note.id);
                }
            }
        }

        if !changed_note_urls.is_empty() {
            for note in notes.iter().filter(|note| note.published) {
                if self.output_contains_any_backlink_source(note, &changed_note_urls) {
                    impacted.insert(note.id);
                }
            }
        }

        let mut changed_asset_hints = HashSet::new();
        for rel in &changed_asset_paths {
            changed_asset_hints.extend(asset_target_tokens(&path_to_unix(rel)));
        }

        if !changed_asset_paths.is_empty() || !changed_asset_hints.is_empty() {
            for note in notes.iter().filter(|note| note.published) {
                let references_changed_asset = dependencies
                    .resolved_asset_targets
                    .get(&note.id)
                    .is_some_and(|targets| {
                        targets
                            .iter()
                            .any(|target| changed_asset_paths.contains(target))
                    });
                let has_unresolved_asset_hit = dependencies
                    .unresolved_asset_targets
                    .get(&note.id)
                    .is_some_and(|targets| {
                        targets
                            .iter()
                            .any(|target| changed_asset_hints.contains(target))
                    });

                if references_changed_asset || has_unresolved_asset_hit {
                    impacted.insert(note.id);
                }
            }
        }

        let mut queue = VecDeque::new();
        for note_id in impacted.iter().copied() {
            queue.push_back(note_id);
        }
        while let Some(note_id) = queue.pop_front() {
            if let Some(sources) = reverse_embed_targets.get(&note_id) {
                for source_id in sources {
                    if !notes[*source_id].published {
                        continue;
                    }
                    if impacted.insert(*source_id) {
                        queue.push_back(*source_id);
                    }
                }
            }
        }

        let mut note_ids = impacted.into_iter().collect::<Vec<_>>();
        note_ids.sort_unstable();

        let existing_assets = assets
            .iter()
            .map(|asset| asset.relative_path.clone())
            .collect::<HashSet<_>>();
        let mut asset_relative_paths = changed_asset_paths
            .into_iter()
            .filter(|path| existing_assets.contains(path))
            .collect::<Vec<_>>();
        asset_relative_paths.sort_by_key(|path| path_to_unix(path));

        IncrementalPlan {
            note_ids,
            asset_relative_paths,
        }
    }

    fn output_contains_any_backlink_source(
        &self,
        note: &RawNote,
        source_urls: &HashSet<String>,
    ) -> bool {
        if source_urls.is_empty() {
            return false;
        }

        let output_path = self
            .runtime
            .output_root
            .join("content")
            .join(&note.relative_path);
        let Ok(rendered) = fs::read_to_string(output_path) else {
            return false;
        };

        source_urls.iter().any(|url| rendered.contains(url))
    }

    fn build_dry_run_report_lines(
        &self,
        processed: &[ProcessedNote],
        asset_relative_paths: &[PathBuf],
    ) -> Vec<String> {
        let mut lines = Vec::new();

        for note in processed {
            lines.push(format!(
                "dry-run: would process note {} -> content/{}",
                note.output_relative_path.display(),
                note.output_relative_path.display()
            ));
        }

        for rel in asset_relative_paths {
            lines.push(format!(
                "dry-run: would process asset {} -> static/assets/{}",
                rel.display(),
                rel.display()
            ));
        }

        lines
    }

    fn build_dry_run_removal_lines(
        &self,
        changed_relative_paths: &HashSet<PathBuf>,
    ) -> Vec<String> {
        let mut changed = changed_relative_paths
            .iter()
            .map(|path| path_to_unix(path))
            .collect::<Vec<_>>();
        changed.sort();

        changed
            .into_iter()
            .map(|rel_unix| {
                let output_rel = self.incremental_output_relative_path(Path::new(&rel_unix));
                format!(
                    "dry-run: would remove stale output {} (if present)",
                    path_to_unix(&output_rel)
                )
            })
            .collect()
    }

    fn scan_vault(&self) -> Result<(Vec<RawNote>, Vec<AssetFile>, Vec<String>)> {
        let mut notes = Vec::new();
        let mut assets = Vec::new();
        let mut errors = Vec::new();

        for entry in WalkDir::new(&self.runtime.vault_root)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| self.should_descend(entry))
        {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    errors.push(format!("failed to scan vault entry: {err}"));
                    continue;
                }
            };
            let path = entry.path();
            if path.is_dir() {
                continue;
            }

            // Reject symlinked files: they could point outside the vault,
            // which would silently publish content the user did not intend.
            if path.symlink_metadata().is_ok_and(|m| m.is_symlink()) {
                self.reporter.trace(format!(
                    "skipping symlink {}",
                    path.display()
                ));
                continue;
            }

            let rel = match path.strip_prefix(&self.runtime.vault_root) {
                Ok(rel) => rel.to_path_buf(),
                Err(err) => {
                    errors.push(format!(
                        "failed to derive relative path for {}: {err}",
                        path.display()
                    ));
                    continue;
                }
            };

            if self.is_ignored_rel(&rel) {
                self.reporter.trace(format!("ignoring {}", rel.display()));
                continue;
            }

            let ext = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();

            if ext == "md" {
                match self.parse_note(path, &rel, notes.len()) {
                    Ok(note) => {
                        self.reporter.trace(format!(
                            "parsed {} (published={})",
                            rel.display(),
                            note.published
                        ));
                        notes.push(note);
                    }
                    Err(err) => errors.push(format!("failed to parse {}: {err:#}", rel.display())),
                }
            } else {
                self.reporter.trace(format!("found asset {}", rel.display()));
                assets.push(AssetFile {
                    source_path: path.to_path_buf(),
                    relative_path: rel,
                });
            }
        }

        Ok((notes, assets, errors))
    }

    fn should_descend(&self, entry: &DirEntry) -> bool {
        if entry.path() == self.runtime.vault_root {
            return true;
        }

        let rel = match entry.path().strip_prefix(&self.runtime.vault_root) {
            Ok(v) => v,
            Err(_) => return true,
        };

        if self.is_ignored_rel(rel) {
            return false;
        }

        !matches!(
            entry.file_name().to_string_lossy().as_ref(),
            ".obsidian" | ".trash"
        )
    }

    fn is_ignored_rel(&self, rel: &Path) -> bool {
        let rel_unix = path_to_unix(rel);
        self.ignore_matcher.is_match(rel_unix)
    }

    fn parse_note(&self, source: &Path, rel: &Path, id: usize) -> Result<RawNote> {
        let raw = fs::read_to_string(source)
            .with_context(|| format!("failed to read markdown file {}", source.display()))?;

        let (frontmatter_raw, body) = split_frontmatter(&raw);
        let mut frontmatter = Mapping::new();
        if let Some(fm_raw) = frontmatter_raw {
            let parsed: YamlValue = serde_yaml::from_str(&fm_raw).with_context(|| {
                format!(
                    "invalid YAML frontmatter in {} (frontmatter starts at first line)",
                    source.display()
                )
            })?;
            if let YamlValue::Mapping(map) = parsed {
                frontmatter = map;
            }
        }

        let stem = rel
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("Untitled")
            .to_string();

        let (mut metadata, published) = parse_metadata(
            &frontmatter,
            &stem,
            self.runtime.app.publish.require_explicit,
        );
        self.validate_metadata_dates(&mut metadata, rel);

        Ok(RawNote {
            id,
            source_path: source.to_path_buf(),
            relative_path: rel.to_path_buf(),
            stem,
            url_path: crate::markdown::url_path_from_relative(rel),
            frontmatter,
            body,
            metadata,
            published,
        })
    }

    fn validate_metadata_dates(&self, metadata: &mut ParsedMetadata, rel: &Path) {
        if let Some(date) = metadata.date.clone()
            && !is_valid_date_value(&date)
        {
            self.reporter.warn(format!(
                "dropping invalid date '{}' in {}",
                date,
                rel.display()
            ));
            metadata.date = None;
        }

        if let Some(updated) = metadata.updated.clone()
            && !is_valid_date_value(&updated)
        {
            self.reporter.warn(format!(
                "dropping invalid updated '{}' in {}",
                updated,
                rel.display()
            ));
            metadata.updated = None;
        }
    }

    fn process_note(
        &self,
        note: &RawNote,
        notes: &[RawNote],
        lookup: &NoteLookup,
        assets: &AssetLookup,
    ) -> Result<ProcessedNote> {
        let mut metadata = note.metadata.clone();
        if self.runtime.app.dataview.effective_mode() == DataviewMode::PreserveToFrontmatter {
            let (_stripped, fields) = extract_dataview_inline_fields(&note.body);
            if !fields.is_empty() {
                merge_dataview_fields_into_extra(&mut metadata.extra, fields);
            }
        }

        let mut outbound_ids = HashSet::new();
        let mut stack = vec![note.id];
        let mut content = self.transform_markdown(
            note.id,
            note.body.clone(),
            notes,
            lookup,
            assets,
            0,
            &mut stack,
            note.id,
            &mut outbound_ids,
        )?;

        content = transform_callouts(&content);

        Ok(ProcessedNote {
            note_id: note.id,
            output_relative_path: note.relative_path.clone(),
            content,
            outbound_ids,
            metadata,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn transform_markdown(
        &self,
        current_id: usize,
        mut input: String,
        notes: &[RawNote],
        lookup: &NoteLookup,
        assets: &AssetLookup,
        depth: usize,
        stack: &mut Vec<usize>,
        root_id: usize,
        outbound_ids: &mut HashSet<usize>,
    ) -> Result<String> {
        if self.runtime.app.markdown.strip_comments {
            input = strip_obsidian_comments(&input);
        }
        if self.runtime.app.markdown.convert_highlights {
            input = convert_highlights(&input);
        }
        match self.runtime.app.dataview.effective_mode() {
            DataviewMode::Strip => {
                input = strip_dataview_inline_fields(&input);
            }
            DataviewMode::PreserveToFrontmatter => {
                input = extract_dataview_inline_fields(&input).0;
            }
            DataviewMode::Preserve => {}
        }
        if self.runtime.app.markdown.soft_line_breaks == SoftLineBreakMode::HardBreaks {
            input = convert_soft_breaks_to_hard(&input);
        }

        input = self.expand_embeds(
            current_id,
            &input,
            notes,
            lookup,
            assets,
            depth,
            stack,
            root_id,
            outbound_ids,
        )?;

        input = self.convert_wikilinks(
            current_id,
            &input,
            notes,
            lookup,
            assets,
            root_id,
            outbound_ids,
        );

        input = self.rewrite_standard_asset_links(current_id, &input, assets, notes, lookup);

        Ok(input)
    }

    #[allow(clippy::too_many_arguments)]
    fn expand_embeds(
        &self,
        current_id: usize,
        input: &str,
        notes: &[RawNote],
        lookup: &NoteLookup,
        assets: &AssetLookup,
        depth: usize,
        stack: &mut Vec<usize>,
        root_id: usize,
        outbound_ids: &mut HashSet<usize>,
    ) -> Result<String> {
        let embed_re = Regex::new(r"!\[\[([^\]]+)\]\]").expect("valid regex");
        let mut out = String::with_capacity(input.len());
        let mut cursor = 0usize;

        for caps in embed_re.captures_iter(input) {
            let m = caps.get(0).expect("match exists");
            let spec_raw = caps.get(1).map(|m| m.as_str()).unwrap_or_default();

            out.push_str(&input[cursor..m.start()]);
            out.push_str(&self.render_embed(
                current_id,
                spec_raw,
                notes,
                lookup,
                assets,
                depth,
                stack,
                root_id,
                outbound_ids,
            )?);
            cursor = m.end();
        }

        out.push_str(&input[cursor..]);
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn render_embed(
        &self,
        current_id: usize,
        spec_raw: &str,
        notes: &[RawNote],
        lookup: &NoteLookup,
        assets: &AssetLookup,
        depth: usize,
        stack: &mut Vec<usize>,
        root_id: usize,
        outbound_ids: &mut HashSet<usize>,
    ) -> Result<String> {
        let spec = parse_wikilink_spec(spec_raw);
        let target = normalize_note_target(&spec.target);

        if is_probably_asset(&target) {
            let path = match self.resolve_asset_reference(
                &notes[current_id].relative_path,
                &target,
                assets,
            ) {
                AssetResolution::Resolved(path) => format!("/assets/{}", path_to_unix(&path)),
                AssetResolution::Ambiguous => {
                    return Ok(format!(
                        "<span class=\"broken-link broken-asset\">Ambiguous asset: {}</span>",
                        html_escape(&target)
                    ));
                }
                AssetResolution::Missing => {
                    return Ok(format!(
                        "<span class=\"broken-link broken-asset\">Missing asset: {}</span>",
                        html_escape(&target)
                    ));
                }
            };
            if let Some(width) = parse_width(spec.alias.as_deref()) {
                return Ok(format!("<img src=\"{path}\" width=\"{width}\" alt=\"\">"));
            }
            return Ok(format!("![]({path})"));
        }

        let Some(target_id) = self.resolve_note_target(current_id, &target, notes, lookup) else {
            return Ok(format!(
                "<span class=\"broken-link\">{}</span>",
                html_escape(spec.alias.as_deref().unwrap_or(&target))
            ));
        };

        if !notes[target_id].published {
            return Ok(format!(
                "<span class=\"broken-link\">{}</span>",
                html_escape(spec.alias.as_deref().unwrap_or(&notes[target_id].stem))
            ));
        }

        if depth >= self.runtime.app.embeds.max_depth {
            return Ok(format!(
                "<div class=\"embed-truncated\">Embed depth limit reached for {}</div>",
                html_escape(&notes[target_id].stem)
            ));
        }

        if stack.contains(&target_id) {
            return Ok(format!(
                "<div class=\"embed-cycle\">Recursive embed skipped for {}</div>",
                html_escape(&notes[target_id].stem)
            ));
        }

        let mut embedded_body = notes[target_id].body.clone();
        if let Some(heading) = spec.heading.as_deref() {
            if let Some(section) = extract_heading_section(&embedded_body, heading) {
                embedded_body = section;
            } else {
                return Ok(format!(
                    "<div class=\"embed-missing-heading\">Missing heading {}</div>",
                    html_escape(heading)
                ));
            }
        }

        stack.push(target_id);
        let rendered = self.transform_markdown(
            target_id,
            embedded_body,
            notes,
            lookup,
            assets,
            depth + 1,
            stack,
            root_id,
            outbound_ids,
        )?;
        stack.pop();

        Ok(format!(
            "<div class=\"transclusion\" data-embed-path=\"{}\">\n{}\n</div>",
            notes[target_id].url_path, rendered
        ))
    }

    fn convert_wikilinks(
        &self,
        current_id: usize,
        input: &str,
        notes: &[RawNote],
        lookup: &NoteLookup,
        assets: &AssetLookup,
        root_id: usize,
        outbound_ids: &mut HashSet<usize>,
    ) -> String {
        let wiki_re = Regex::new(r"\[\[([^\]]+)\]\]").expect("valid regex");
        let mut out = String::with_capacity(input.len());
        let mut cursor = 0usize;
        let bytes = input.as_bytes();

        for caps in wiki_re.captures_iter(input) {
            let m = caps.get(0).expect("match exists");
            let spec_raw = caps.get(1).map(|m| m.as_str()).unwrap_or_default();

            out.push_str(&input[cursor..m.start()]);
            cursor = m.end();

            if m.start() > 0 && bytes[m.start() - 1] == b'!' {
                out.push_str(m.as_str());
                continue;
            }

            let spec = parse_wikilink_spec(spec_raw);
            let target = normalize_note_target(&spec.target);
            let visible = spec.alias.as_deref().unwrap_or_else(|| {
                if target.is_empty() {
                    "broken"
                } else {
                    target.rsplit('/').next().unwrap_or(&target)
                }
            });

            if is_probably_asset(&target) {
                match self.resolve_asset_reference(
                    &notes[current_id].relative_path,
                    &target,
                    assets,
                ) {
                    AssetResolution::Resolved(path) => {
                        out.push_str(&format!("[{}](/assets/{})", visible, path_to_unix(&path)));
                    }
                    AssetResolution::Ambiguous => out.push_str(&format!(
                        "<span class=\"broken-link broken-asset\">{}</span>",
                        html_escape(visible)
                    )),
                    AssetResolution::Missing => out.push_str(&format!(
                        "<span class=\"broken-link broken-asset\">{}</span>",
                        html_escape(visible)
                    )),
                }
                continue;
            }

            let Some(target_id) = self.resolve_note_target(current_id, &target, notes, lookup)
            else {
                out.push_str(&format!(
                    "<span class=\"broken-link\">{}</span>",
                    html_escape(visible)
                ));
                continue;
            };

            if !notes[target_id].published {
                out.push_str(&format!(
                    "<span class=\"broken-link\">{}</span>",
                    html_escape(visible)
                ));
                continue;
            }

            if root_id == current_id {
                outbound_ids.insert(target_id);
            }

            let mut href = notes[target_id].url_path.clone();
            if let Some(heading) = spec.heading.as_deref() {
                href.push('#');
                href.push_str(&slugify(heading));
            }

            out.push_str(&format!("[{}]({})", visible, href));
        }

        out.push_str(&input[cursor..]);
        out
    }

    fn rewrite_standard_asset_links(
        &self,
        current_id: usize,
        input: &str,
        assets: &AssetLookup,
        notes: &[RawNote],
        lookup: &NoteLookup,
    ) -> String {
        let image_re = Regex::new(r"!\[([^\]]*)\]\(([^)]+)\)").expect("valid regex");
        let link_re = Regex::new(r"\[([^\]]+)\]\(([^)]+)\)").expect("valid regex");

        let after_images = image_re
            .replace_all(input, |caps: &Captures| {
                let alt = caps.get(1).map(|m| m.as_str()).unwrap_or_default();
                let target_raw = caps.get(2).map(|m| m.as_str()).unwrap_or_default();
                let target = markdown_link_target(target_raw);
                if is_external_or_anchor(target.as_ref()) {
                    return caps
                        .get(0)
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_default();
                }

                let (target_path, fragment) = split_link_fragment(target.as_ref());
                match self.resolve_asset_reference(
                    &notes[current_id].relative_path,
                    target_path,
                    assets,
                ) {
                    AssetResolution::Resolved(rel) => {
                        let mut href = format!("/assets/{}", path_to_unix(&rel));
                        if let Some(fragment) = fragment.filter(|f| !f.is_empty()) {
                            href.push('#');
                            href.push_str(fragment);
                        }
                        format!("![{}]({})", alt, href)
                    }
                    AssetResolution::Ambiguous => {
                        self.reporter.warn(format!(
                            "ambiguous asset link '{}' in {}",
                            target_path,
                            notes[current_id].relative_path.display()
                        ));
                        caps.get(0)
                            .map(|m| m.as_str().to_string())
                            .unwrap_or_default()
                    }
                    AssetResolution::Missing => caps
                        .get(0)
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_default(),
                }
            })
            .to_string();

        link_re
            .replace_all(&after_images, |caps: &Captures| {
                let entire = caps.get(0).map(|m| m.as_str()).unwrap_or_default();
                if entire.starts_with("![") {
                    return entire.to_string();
                }

                let label = caps.get(1).map(|m| m.as_str()).unwrap_or_default();
                let target_raw = caps.get(2).map(|m| m.as_str()).unwrap_or_default();
                let target = markdown_link_target(target_raw);
                if is_external_or_anchor(target.as_ref()) {
                    return entire.to_string();
                }

                let (target_path, fragment) = split_link_fragment(target.as_ref());

                if target_path.to_ascii_lowercase().ends_with(".md") {
                    let normalized = normalize_note_target(target_path);
                    if let Some(target_id) =
                        self.resolve_note_target(current_id, &normalized, notes, lookup)
                    {
                        if notes[target_id].published {
                            let mut href = notes[target_id].url_path.clone();
                            if let Some(fragment) = fragment.filter(|f| !f.is_empty()) {
                                href.push('#');
                                href.push_str(&slugify(fragment));
                            }
                            return format!("[{label}]({href})");
                        }
                    }
                }

                match self.resolve_asset_reference(
                    &notes[current_id].relative_path,
                    target_path,
                    assets,
                ) {
                    AssetResolution::Resolved(rel) => {
                        let mut href = format!("/assets/{}", path_to_unix(&rel));
                        if let Some(fragment) = fragment.filter(|f| !f.is_empty()) {
                            href.push('#');
                            href.push_str(fragment);
                        }
                        return format!("[{label}]({href})");
                    }
                    AssetResolution::Ambiguous => {
                        self.reporter.warn(format!(
                            "ambiguous asset link '{}' in {}",
                            target_path,
                            notes[current_id].relative_path.display()
                        ));
                    }
                    AssetResolution::Missing => {}
                }

                entire.to_string()
            })
            .to_string()
    }

    fn resolve_asset_reference(
        &self,
        current_relative_path: &Path,
        target: &str,
        assets: &AssetLookup,
    ) -> AssetResolution {
        if target.trim().is_empty() {
            return AssetResolution::Missing;
        }

        if let Some(stripped) = target.strip_prefix('/') {
            let rel = PathBuf::from(stripped);
            return if assets.paths.contains(&rel) {
                AssetResolution::Resolved(rel)
            } else {
                AssetResolution::Missing
            };
        }

        if target.contains('/') || target.starts_with('.') {
            let Some(normalized) = normalize_relative_link(current_relative_path, target) else {
                return AssetResolution::Missing;
            };

            return if assets.paths.contains(&normalized) {
                AssetResolution::Resolved(normalized)
            } else {
                AssetResolution::Missing
            };
        }

        let Some(relative_candidate) = normalize_relative_link(current_relative_path, target)
        else {
            return AssetResolution::Missing;
        };
        if assets.paths.contains(&relative_candidate) {
            return AssetResolution::Resolved(relative_candidate);
        }

        let basename = Path::new(target)
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.to_ascii_lowercase());

        let Some(basename) = basename else {
            return AssetResolution::Missing;
        };

        match assets.by_basename.get(&basename) {
            None => AssetResolution::Missing,
            Some(candidates) if candidates.is_empty() => AssetResolution::Missing,
            Some(candidates) if candidates.len() == 1 => {
                AssetResolution::Resolved(candidates[0].clone())
            }
            Some(_) => AssetResolution::Ambiguous,
        }
    }

    fn resolve_note_target(
        &self,
        current_id: usize,
        target: &str,
        notes: &[RawNote],
        lookup: &NoteLookup,
    ) -> Option<usize> {
        if target.is_empty() {
            return None;
        }

        let normalized = normalize_note_target(target);
        let normalized_lower = normalized.to_ascii_lowercase();

        if let Some(stripped) = normalized.strip_prefix('/') {
            let mut key = stripped.to_ascii_lowercase();
            if key.ends_with(".md") {
                key.truncate(key.len().saturating_sub(3));
            }
            if let Some(id) = lookup.by_path.get(&key) {
                return Some(*id);
            }
        }

        if normalized.contains('/') || normalized.starts_with('.') {
            if let Some(relative) =
                normalize_relative_link(&notes[current_id].relative_path, &normalized)
            {
                let mut key = path_to_unix(&relative).to_ascii_lowercase();
                if key.ends_with(".md") {
                    key.truncate(key.len().saturating_sub(3));
                }
                if let Some(id) = lookup.by_path.get(&key) {
                    return Some(*id);
                }
            }
        }

        if normalized_lower.contains('/') {
            let mut key = normalized_lower.clone();
            if key.ends_with(".md") {
                key.truncate(key.len().saturating_sub(3));
            }
            if let Some(id) = lookup.by_path.get(&key) {
                return Some(*id);
            }
        }

        let stem = normalized_lower
            .rsplit('/')
            .next()
            .unwrap_or(&normalized_lower)
            .to_ascii_lowercase();
        let candidates = lookup.by_stem.get(&stem)?;

        if candidates.len() == 1 {
            return candidates.first().copied();
        }

        let current_parent = notes[current_id]
            .relative_path
            .parent()
            .unwrap_or_else(|| Path::new(""));

        // Prefer published notes over unpublished ones so that a closer
        // unpublished note doesn't shadow a valid published target.
        candidates.iter().copied().min_by_key(|candidate_id| {
            let note = &notes[*candidate_id];
            let unpublished = if note.published { 0u8 } else { 1u8 };
            let distance = path_distance_between(current_parent, &note.relative_path);
            let rel = path_to_unix(&note.relative_path);
            (unpublished, distance, rel)
        })
    }

    fn build_graph_and_backlinks_from_dependencies(
        &self,
        notes: &[RawNote],
        dependencies: &NoteDependencyScan,
        published_ids: &HashSet<usize>,
    ) -> (GraphData, HashMap<usize, Vec<BacklinkItem>>) {
        let mut edge_set: BTreeSet<(usize, usize)> = BTreeSet::new();
        let mut backlinks: HashMap<usize, BTreeSet<usize>> = HashMap::new();

        for (source_id, targets) in &dependencies.backlink_targets {
            for target_id in targets {
                if published_ids.contains(target_id) {
                    edge_set.insert((*source_id, *target_id));
                    backlinks.entry(*target_id).or_default().insert(*source_id);
                }
            }
        }

        let mut nodes = Vec::new();
        for note in notes.iter().filter(|n| n.published) {
            nodes.push(GraphNode {
                id: note.url_path.clone(),
                title: note
                    .metadata
                    .title
                    .clone()
                    .unwrap_or_else(|| note.stem.clone()),
                path: note.url_path.clone(),
                tags: note.metadata.tags.clone(),
                kind: "note".to_string(),
            });
        }

        let mut edges = Vec::new();
        for (source_id, target_id) in edge_set {
            edges.push(GraphEdge {
                source: notes[source_id].url_path.clone(),
                target: notes[target_id].url_path.clone(),
            });
        }

        if self.runtime.app.graph.include_tag_nodes {
            let mut tags = BTreeSet::new();
            for note in notes.iter().filter(|n| n.published) {
                for tag in &note.metadata.tags {
                    tags.insert(tag.clone());
                    edges.push(GraphEdge {
                        source: note.url_path.clone(),
                        target: format!("tag:{tag}"),
                    });
                }
            }
            for tag in tags {
                nodes.push(GraphNode {
                    id: format!("tag:{tag}"),
                    title: tag.clone(),
                    path: format!("/tags/{}/", slugify(&tag)),
                    tags: Vec::new(),
                    kind: "tag".to_string(),
                });
            }
        }

        let backlink_map = backlinks
            .into_iter()
            .map(|(target_id, sources)| {
                let items = sources
                    .into_iter()
                    .map(|source_id| BacklinkItem {
                        title: notes[source_id]
                            .metadata
                            .title
                            .clone()
                            .unwrap_or_else(|| notes[source_id].stem.clone()),
                        path: notes[source_id].url_path.clone(),
                    })
                    .collect::<Vec<_>>();
                (target_id, items)
            })
            .collect::<HashMap<_, _>>();

        (GraphData { nodes, edges }, backlink_map)
    }

    fn prepare_output_dirs(&self) -> Result<()> {
        let content_dir = self.runtime.output_root.join("content");
        let assets_dir = self.runtime.output_root.join("static").join("assets");
        let graph_path = self.runtime.output_root.join("static").join("graph.json");

        if content_dir.exists() {
            fs::remove_dir_all(&content_dir)
                .with_context(|| format!("failed to clean {}", content_dir.display()))?;
        }
        if assets_dir.exists() {
            fs::remove_dir_all(&assets_dir)
                .with_context(|| format!("failed to clean {}", assets_dir.display()))?;
        }
        if graph_path.exists() {
            fs::remove_file(&graph_path)
                .with_context(|| format!("failed to clean {}", graph_path.display()))?;
        }

        fs::create_dir_all(&content_dir)
            .with_context(|| format!("failed to create {}", content_dir.display()))?;
        fs::create_dir_all(&assets_dir)
            .with_context(|| format!("failed to create {}", assets_dir.display()))?;
        Ok(())
    }

    fn prepare_output_dirs_incremental(&self) -> Result<()> {
        let content_dir = self.runtime.output_root.join("content");
        let assets_dir = self.runtime.output_root.join("static").join("assets");

        fs::create_dir_all(&content_dir)
            .with_context(|| format!("failed to create {}", content_dir.display()))?;
        fs::create_dir_all(&assets_dir)
            .with_context(|| format!("failed to create {}", assets_dir.display()))?;
        Ok(())
    }

    fn remove_stale_changed_outputs(
        &self,
        changed_relative_paths: &HashSet<PathBuf>,
    ) -> Result<()> {
        for rel in changed_relative_paths {
            let target = self
                .runtime
                .output_root
                .join(self.incremental_output_relative_path(rel));
            self.remove_path_if_exists(&target)?;
        }

        Ok(())
    }

    fn incremental_output_relative_path(&self, rel: &Path) -> PathBuf {
        let ext = rel
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();

        if ext == "md" {
            PathBuf::from("content").join(rel)
        } else {
            PathBuf::from("static").join("assets").join(rel)
        }
    }

    fn remove_path_if_exists(&self, path: &Path) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }

        if path.is_dir() {
            fs::remove_dir_all(path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        } else {
            fs::remove_file(path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }

        Ok(())
    }

    fn write_assets(&self, assets: &[AssetFile]) -> Result<()> {
        for asset in assets {
            self.write_asset_file(asset)?;
        }
        Ok(())
    }

    fn write_assets_incremental(
        &self,
        assets: &[AssetFile],
        changed_asset_paths: &[PathBuf],
    ) -> Result<()> {
        let changed = changed_asset_paths.iter().cloned().collect::<HashSet<_>>();
        for asset in assets {
            if changed.contains(&asset.relative_path) {
                self.write_asset_file(asset)?;
            }
        }
        Ok(())
    }

    fn write_asset_file(&self, asset: &AssetFile) -> Result<()> {
        let target = self
            .runtime
            .output_root
            .join("static")
            .join("assets")
            .join(&asset.relative_path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create asset dir {}", parent.display()))?;
        }
        fs::copy(&asset.source_path, &target).with_context(|| {
            format!(
                "failed to copy asset {} -> {}",
                asset.source_path.display(),
                target.display()
            )
        })?;
        Ok(())
    }

    fn write_notes(
        &self,
        notes: &[RawNote],
        processed: &[ProcessedNote],
        backlinks: &HashMap<usize, Vec<BacklinkItem>>,
    ) -> Result<()> {
        for note in processed {
            let source = &notes[note.note_id];
            let path_key = source
                .url_path
                .trim_start_matches('/')
                .trim_end_matches('/')
                .to_string();
            let rendered = render_note_with_frontmatter(
                &note.content,
                &note.metadata,
                &source.stem,
                &path_key,
                backlinks.get(&note.note_id),
                self.runtime.app.frontmatter.output.clone(),
            )?;

            let target = self
                .runtime
                .output_root
                .join("content")
                .join(&note.output_relative_path);

            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create content dir {}", parent.display())
                })?;
            }

            fs::write(&target, rendered)
                .with_context(|| format!("failed to write note {}", target.display()))?;
        }

        Ok(())
    }

    fn write_graph(&self, graph: &GraphData) -> Result<()> {
        let graph_path = self.runtime.output_root.join("static").join("graph.json");
        if let Some(parent) = graph_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let data = serde_json::to_string_pretty(graph).context("failed to serialize graph.json")?;
        fs::write(&graph_path, data)
            .with_context(|| format!("failed to write {}", graph_path.display()))?;
        Ok(())
    }

    fn write_section_indexes(&self, processed: &[ProcessedNote]) -> Result<()> {
        let content_root = self.runtime.output_root.join("content");
        let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
        dirs.insert(PathBuf::new()); // root section

        for note in processed {
            let mut parent = note.output_relative_path.parent().map(Path::to_path_buf);
            while let Some(dir) = parent {
                dirs.insert(dir.clone());
                parent = dir.parent().map(Path::to_path_buf);
            }
        }

        for dir in dirs {
            let index_path = content_root.join(&dir).join("_index.md");
            if index_path.exists() {
                continue;
            }
            if let Some(parent) = index_path.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create section index dir {}", parent.display())
                })?;
            }

            let title = if dir.as_os_str().is_empty() {
                "Home".to_string()
            } else {
                dir.file_name()
                    .and_then(|s| s.to_str())
                    .map(|s| s.replace(['-', '_'], " "))
                    .map(title_case)
                    .unwrap_or_else(|| "Section".to_string())
            };

            let content = format!("+++\ntitle = \"{}\"\nsort_by = \"title\"\n+++\n", title);
            fs::write(&index_path, content).with_context(|| {
                format!("failed to write section index {}", index_path.display())
            })?;
        }

        Ok(())
    }

    fn prune_empty_section_indexes(&self, all_published: &[ProcessedNote]) -> Result<()> {
        let content_root = self.runtime.output_root.join("content");
        if !content_root.exists() {
            return Ok(());
        }

        // Collect all directories that should have _index.md files.
        let mut live_dirs: BTreeSet<PathBuf> = BTreeSet::new();
        live_dirs.insert(PathBuf::new()); // root always lives
        for note in all_published {
            let mut parent = note.output_relative_path.parent().map(Path::to_path_buf);
            while let Some(dir) = parent {
                live_dirs.insert(dir.clone());
                parent = dir.parent().map(Path::to_path_buf);
            }
        }

        // Walk content/ and remove _index.md in directories that are not in the live set.
        for entry in WalkDir::new(&content_root)
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();
            if path.file_name().and_then(|s| s.to_str()) != Some("_index.md") {
                continue;
            }
            let dir = path
                .parent()
                .and_then(|p| p.strip_prefix(&content_root).ok())
                .map(Path::to_path_buf)
                .unwrap_or_default();

            if !live_dirs.contains(&dir) {
                fs::remove_file(path).with_context(|| {
                    format!("failed to remove stale section index {}", path.display())
                })?;
            }
        }

        Ok(())
    }
}

fn build_ignore_matcher(globs: &[String]) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in globs {
        builder.add(Glob::new(pattern).with_context(|| format!("invalid ignore glob: {pattern}"))?);
    }
    Ok(builder
        .build()
        .context("failed to build ignore glob matcher")?)
}

fn parse_metadata(
    frontmatter: &Mapping,
    fallback_title: &str,
    require_explicit_publish: bool,
) -> (ParsedMetadata, bool) {
    let mut map = frontmatter.clone();

    let title = take_string(&mut map, &["title"]).or_else(|| Some(fallback_title.to_string()));
    let tags = take_string_list(&mut map, &["tags"]);
    let aliases = take_string_list(&mut map, &["aliases"]);
    let css_classes = {
        let mut classes = take_string_list(&mut map, &["cssclass"]);
        classes.extend(take_string_list(&mut map, &["cssclasses"]));
        classes
    };
    let date = take_string(&mut map, &["date"]).or_else(|| take_string(&mut map, &["created"]));
    let updated =
        take_string(&mut map, &["updated"]).or_else(|| take_string(&mut map, &["modified"]));
    let publish = take_bool(&mut map, &["publish"]);

    let published = if require_explicit_publish {
        publish.unwrap_or(false)
    } else {
        publish.unwrap_or(true)
    };

    let mut extra = BTreeMap::new();
    for (k, v) in map {
        if let Some(key) = yaml_key_to_string(&k)
            && let Some(tv) = yaml_to_toml(&v)
        {
            extra.insert(key, tv);
        }
    }

    (
        ParsedMetadata {
            title,
            tags,
            aliases,
            css_classes,
            date,
            updated,
            extra,
        },
        published,
    )
}

fn merge_dataview_fields_into_extra(
    extra: &mut BTreeMap<String, toml::Value>,
    fields: Vec<(String, String)>,
) {
    for (key, value) in fields {
        if key.is_empty() {
            continue;
        }

        match extra.get_mut(&key) {
            None => {
                extra.insert(key, toml::Value::String(value));
            }
            Some(existing @ toml::Value::String(_)) => {
                let previous = std::mem::replace(existing, toml::Value::Array(Vec::new()));
                if let toml::Value::Array(items) = existing {
                    items.push(previous);
                    items.push(toml::Value::String(value));
                }
            }
            Some(toml::Value::Array(items)) => {
                items.push(toml::Value::String(value));
            }
            Some(other) => {
                *other = toml::Value::String(value);
            }
        }
    }
}

fn take_string(map: &mut Mapping, keys: &[&str]) -> Option<String> {
    let value = remove_key_case_insensitive(map, keys)?;
    yaml_scalar_to_string(&value)
}

fn take_bool(map: &mut Mapping, keys: &[&str]) -> Option<bool> {
    let value = remove_key_case_insensitive(map, keys)?;
    match value {
        YamlValue::Bool(v) => Some(v),
        YamlValue::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" => Some(true),
            "false" | "no" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn take_string_list(map: &mut Mapping, keys: &[&str]) -> Vec<String> {
    let Some(value) = remove_key_case_insensitive(map, keys) else {
        return Vec::new();
    };
    match value {
        YamlValue::String(s) => {
            if s.contains(',') {
                s.split(',')
                    .map(|part| part.trim().to_string())
                    .filter(|v| !v.is_empty())
                    .collect()
            } else {
                vec![s]
            }
        }
        YamlValue::Sequence(items) => items
            .into_iter()
            .filter_map(|item| yaml_scalar_to_string(&item))
            .collect(),
        _ => Vec::new(),
    }
}

fn remove_key_case_insensitive(map: &mut Mapping, keys: &[&str]) -> Option<YamlValue> {
    let expected: Vec<String> = keys.iter().map(|k| k.to_ascii_lowercase()).collect();
    let hit = map.keys().find_map(|key| {
        let key_str = yaml_key_to_string(key)?;
        expected
            .iter()
            .any(|expected| expected == &key_str.to_ascii_lowercase())
            .then(|| key.clone())
    })?;

    map.remove(&hit)
}

fn yaml_key_to_string(value: &YamlValue) -> Option<String> {
    match value {
        YamlValue::String(s) => Some(s.clone()),
        YamlValue::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn yaml_scalar_to_string(value: &YamlValue) -> Option<String> {
    match value {
        YamlValue::String(s) => Some(s.clone()),
        YamlValue::Number(n) => Some(n.to_string()),
        YamlValue::Bool(v) => Some(v.to_string()),
        _ => None,
    }
}

fn yaml_to_toml(value: &YamlValue) -> Option<toml::Value> {
    match value {
        YamlValue::Null => None,
        YamlValue::Bool(v) => Some(toml::Value::Boolean(*v)),
        YamlValue::Number(n) => {
            if let Some(v) = n.as_i64() {
                Some(toml::Value::Integer(v))
            } else if let Some(v) = n.as_u64() {
                i64::try_from(v).ok().map(toml::Value::Integer)
            } else {
                n.as_f64().map(toml::Value::Float)
            }
        }
        YamlValue::String(s) => Some(toml::Value::String(s.clone())),
        YamlValue::Sequence(items) => {
            let arr = items.iter().filter_map(yaml_to_toml).collect::<Vec<_>>();
            Some(toml::Value::Array(arr))
        }
        YamlValue::Mapping(map) => {
            let mut out = toml::map::Map::new();
            for (k, v) in map {
                if let Some(key) = yaml_key_to_string(k)
                    && let Some(tv) = yaml_to_toml(v)
                {
                    out.insert(key, tv);
                }
            }
            Some(toml::Value::Table(out))
        }
        YamlValue::Tagged(tagged) => yaml_to_toml(&tagged.value),
    }
}

fn path_to_unix(path: &Path) -> String {
    path.components()
        .filter_map(|comp| match comp {
            std::path::Component::Normal(p) => Some(p.to_string_lossy().to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn path_distance_between(from_dir: &Path, to_file: &Path) -> usize {
    let from = path_to_unix(from_dir);
    let to = path_to_unix(to_file);
    let from_parts = from
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let to_parts = to
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();

    let mut common = 0usize;
    while common < from_parts.len()
        && common < to_parts.len()
        && from_parts[common].eq_ignore_ascii_case(to_parts[common])
    {
        common += 1;
    }

    (from_parts.len().saturating_sub(common)) + (to_parts.len().saturating_sub(common))
}

fn build_lookup(notes: &[RawNote]) -> NoteLookup {
    let mut by_stem: HashMap<String, Vec<usize>> = HashMap::new();
    let mut by_path: HashMap<String, usize> = HashMap::new();

    for note in notes {
        by_stem
            .entry(note.stem.to_ascii_lowercase())
            .or_default()
            .push(note.id);

        let mut rel = path_to_unix(&note.relative_path).to_ascii_lowercase();
        if rel.ends_with(".md") {
            rel.truncate(rel.len().saturating_sub(3));
        }
        by_path.insert(rel, note.id);
    }

    NoteLookup { by_stem, by_path }
}

fn build_asset_lookup(assets: &[AssetFile]) -> AssetLookup {
    let mut paths: HashSet<PathBuf> = HashSet::new();
    let mut by_basename: HashMap<String, Vec<PathBuf>> = HashMap::new();

    for asset in assets {
        paths.insert(asset.relative_path.clone());

        if let Some(basename) = asset.relative_path.file_name().and_then(|s| s.to_str()) {
            by_basename
                .entry(basename.to_ascii_lowercase())
                .or_default()
                .push(asset.relative_path.clone());
        }
    }

    for entries in by_basename.values_mut() {
        entries.sort_by_key(|path| {
            let depth = path.components().count();
            let key = path_to_unix(path);
            (depth, key)
        });
    }

    AssetLookup { paths, by_basename }
}

fn reverse_note_dependencies(
    map: &HashMap<usize, HashSet<usize>>,
) -> HashMap<usize, HashSet<usize>> {
    let mut reverse = HashMap::new();
    for (source, targets) in map {
        for target in targets {
            reverse
                .entry(*target)
                .or_insert_with(HashSet::new)
                .insert(*source);
        }
    }
    reverse
}

fn note_target_tokens(target: &str) -> HashSet<String> {
    let normalized = normalize_note_target(target).to_ascii_lowercase();
    let mut tokens = HashSet::new();
    if normalized.is_empty() {
        return tokens;
    }

    tokens.insert(normalized.clone());
    if let Some(stem) = normalized.rsplit('/').next()
        && !stem.is_empty()
    {
        tokens.insert(stem.to_string());
    }
    tokens
}

fn note_target_tokens_for_relative_path(relative_path: &Path) -> HashSet<String> {
    let mut normalized = path_to_unix(relative_path).to_ascii_lowercase();
    if normalized.ends_with(".md") {
        normalized.truncate(normalized.len().saturating_sub(3));
    }
    note_target_tokens(&normalized)
}

fn asset_target_tokens(target: &str) -> HashSet<String> {
    let mut normalized = target.trim().replace('\\', "/").to_ascii_lowercase();
    while normalized.starts_with("./") {
        normalized = normalized.trim_start_matches("./").to_string();
    }
    if let Some(stripped) = normalized.strip_prefix('/') {
        normalized = stripped.to_string();
    }

    let mut tokens = HashSet::new();
    if normalized.is_empty() {
        return tokens;
    }

    tokens.insert(normalized.clone());
    if let Some(stem) = Path::new(&normalized).file_name().and_then(|s| s.to_str())
        && !stem.is_empty()
    {
        tokens.insert(stem.to_ascii_lowercase());
    }
    tokens
}

fn split_link_fragment(target: &str) -> (&str, Option<&str>) {
    if let Some((path, fragment)) = target.split_once('#') {
        (path, Some(fragment.trim()))
    } else {
        (target, None)
    }
}

fn is_valid_date_value(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return false;
    }

    if is_valid_iso_date(trimmed) {
        return true;
    }

    trimmed.parse::<toml::value::Datetime>().is_ok()
}

fn is_valid_iso_date(value: &str) -> bool {
    if value.len() != 10 {
        return false;
    }
    let bytes = value.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return false;
    }

    let year = value[0..4].parse::<i32>().ok();
    let month = value[5..7].parse::<u32>().ok();
    let day = value[8..10].parse::<u32>().ok();
    let (Some(year), Some(month), Some(day)) = (year, month, day) else {
        return false;
    };
    if !(1..=12).contains(&month) || day == 0 {
        return false;
    }

    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => return false,
    };

    day <= days_in_month
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn is_probably_asset(target: &str) -> bool {
    let lower = target.to_ascii_lowercase();
    [
        ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".pdf", ".avif", ".mp4", ".mp3", ".ogg",
        ".mov", ".webm",
    ]
    .iter()
    .any(|ext| lower.ends_with(ext))
}

fn parse_width(alias: Option<&str>) -> Option<u32> {
    let alias = alias?.trim();
    if alias.is_empty() {
        return None;
    }
    let value = alias.split('x').next().unwrap_or(alias).trim();
    value.parse::<u32>().ok()
}

fn title_case(input: String) -> String {
    input
        .split_whitespace()
        .map(|part| {
            let mut chars = part.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };
            format!(
                "{}{}",
                first.to_ascii_uppercase(),
                chars.as_str().to_ascii_lowercase()
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn render_note_with_frontmatter(
    body: &str,
    metadata: &ParsedMetadata,
    fallback_title: &str,
    path_key: &str,
    backlinks: Option<&Vec<BacklinkItem>>,
    output_format: FrontmatterOutput,
) -> Result<String> {
    let mut root = toml::map::Map::new();
    root.insert(
        "title".to_string(),
        toml::Value::String(
            metadata
                .title
                .clone()
                .unwrap_or_else(|| fallback_title.to_string()),
        ),
    );

    if !path_key.is_empty() {
        root.insert(
            "path".to_string(),
            toml::Value::String(path_key.to_string()),
        );
    }

    if let Some(date) = metadata.date.as_ref() {
        root.insert("date".to_string(), toml::Value::String(date.clone()));
    }

    if let Some(updated) = metadata.updated.as_ref() {
        root.insert("updated".to_string(), toml::Value::String(updated.clone()));
    }

    if !metadata.tags.is_empty() {
        let mut taxonomies = toml::map::Map::new();
        taxonomies.insert(
            "tags".to_string(),
            toml::Value::Array(
                metadata
                    .tags
                    .iter()
                    .map(|t| toml::Value::String(t.clone()))
                    .collect(),
            ),
        );
        root.insert("taxonomies".to_string(), toml::Value::Table(taxonomies));
    }

    let mut extra = toml::map::Map::new();
    if !metadata.aliases.is_empty() {
        extra.insert(
            "aliases".to_string(),
            toml::Value::Array(
                metadata
                    .aliases
                    .iter()
                    .map(|v| toml::Value::String(v.clone()))
                    .collect(),
            ),
        );
    }

    if !metadata.css_classes.is_empty() {
        extra.insert(
            "css_classes".to_string(),
            toml::Value::Array(
                metadata
                    .css_classes
                    .iter()
                    .map(|v| toml::Value::String(v.clone()))
                    .collect(),
            ),
        );
    }

    for (k, v) in metadata.extra.iter() {
        extra.insert(k.clone(), v.clone());
    }

    let backlink_values = backlinks
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    let mut table = toml::map::Map::new();
                    table.insert("title".to_string(), toml::Value::String(item.title.clone()));
                    table.insert("path".to_string(), toml::Value::String(item.path.clone()));
                    toml::Value::Table(table)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    extra.insert("backlinks".to_string(), toml::Value::Array(backlink_values));

    root.insert("extra".to_string(), toml::Value::Table(extra));

    match output_format {
        FrontmatterOutput::Toml => {
            let fm = toml::to_string_pretty(&root).context("failed to write TOML frontmatter")?;
            Ok(format!("+++\n{}+++\n\n{}\n", fm, body.trim_end()))
        }
        FrontmatterOutput::Yaml => {
            let yaml_value = toml_to_yaml(toml::Value::Table(root));
            let fm =
                serde_yaml::to_string(&yaml_value).context("failed to write YAML frontmatter")?;
            // serde_yaml::to_string emits a leading "---\n"; strip it so we
            // control the fences ourselves and avoid doubling.
            let fm = fm.strip_prefix("---\n").unwrap_or(&fm);
            Ok(format!("---\n{}---\n\n{}\n", fm, body.trim_end()))
        }
    }
}

fn toml_to_yaml(value: toml::Value) -> YamlValue {
    match value {
        toml::Value::String(v) => YamlValue::String(v),
        toml::Value::Integer(v) => YamlValue::Number(v.into()),
        toml::Value::Float(v) => YamlValue::String(v.to_string()),
        toml::Value::Boolean(v) => YamlValue::Bool(v),
        toml::Value::Array(items) => {
            YamlValue::Sequence(items.into_iter().map(toml_to_yaml).collect())
        }
        toml::Value::Table(map) => {
            let mut out = Mapping::new();
            for (k, v) in map {
                out.insert(YamlValue::String(k), toml_to_yaml(v));
            }
            YamlValue::Mapping(out)
        }
        toml::Value::Datetime(v) => YamlValue::String(v.to_string()),
    }
}

pub fn run_publisher(runtime: RuntimeConfig) -> Result<RunSummary> {
    let reporter = Reporter::new(runtime.verbose, runtime.quiet);
    let publisher = Publisher::new(runtime, reporter)?;
    publisher.run_once()
}

pub fn run_publisher_incremental(
    runtime: RuntimeConfig,
    changed_paths: &[PathBuf],
) -> Result<RunSummary> {
    let reporter = Reporter::new(runtime.verbose, runtime.quiet);
    let publisher = Publisher::new(runtime, reporter)?;
    publisher.run_incremental(changed_paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AppConfig, DataviewConfig, DataviewMode, EmbedConfig, FrontmatterConfig, GraphConfig,
        MarkdownConfig, PublishConfig, SoftLineBreakMode,
    };
    use tempfile::TempDir;

    fn runtime(vault: &Path, out: &Path) -> RuntimeConfig {
        RuntimeConfig {
            vault_root: vault.to_path_buf(),
            output_root: out.to_path_buf(),
            app: AppConfig {
                publish: PublishConfig {
                    require_explicit: true,
                },
                ignore_globs: vec![".obsidian/**".to_string()],
                frontmatter: FrontmatterConfig {
                    output: FrontmatterOutput::Toml,
                },
                graph: GraphConfig {
                    include_tag_nodes: false,
                    enabled: true,
                },
                dataview: DataviewConfig {
                    mode: DataviewMode::Strip,
                    strip_inline_fields: Some(true),
                },
                embeds: EmbedConfig { max_depth: 3 },
                markdown: MarkdownConfig {
                    strip_comments: true,
                    convert_highlights: true,
                    soft_line_breaks: SoftLineBreakMode::Preserve,
                },
                watch: crate::config::WatchConfig { debounce_ms: 200 },
            },
            dry_run: false,
            watch: false,
            verbose: 0,
            quiet: true,
        }
    }

    #[test]
    fn integration_pipeline_writes_outputs() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(vault.join("nested")).expect("mkdir");

        fs::write(
            vault.join("Home.md"),
            "---\npublish: true\ntitle: Home\ntags: [rust]\n---\nSee [[nested/Child|child]].\n![[nested/Child#Section]]\n",
        )
        .expect("write Home");

        fs::write(
            vault.join("nested/Child.md"),
            "---\npublish: true\n---\n# Section\ncontent ==hi==\n",
        )
        .expect("write Child");

        fs::write(vault.join("pic.png"), "binary").expect("write pic");

        let summary = run_publisher(runtime(&vault, &out)).expect("publish success");
        assert_eq!(summary.notes_published, 2);

        let home = fs::read_to_string(out.join("content/Home.md")).expect("home exists");
        assert!(home.contains("[child](/nested/child/)"));
        assert!(home.contains("<mark>hi</mark>"));

        let graph = fs::read_to_string(out.join("static/graph.json")).expect("graph exists");
        assert!(graph.contains("/home/"));
        assert!(graph.contains("/nested/child/"));

        assert!(out.join("static/assets/pic.png").exists());
    }

    #[test]
    fn parse_metadata_respects_publish_filter() {
        let yaml: YamlValue = serde_yaml::from_str("publish: false\ntitle: X").expect("yaml");
        let YamlValue::Mapping(map) = yaml else {
            panic!("expected mapping")
        };

        let (meta, published) = parse_metadata(&map, "Fallback", true);
        assert_eq!(meta.title.as_deref(), Some("X"));
        assert!(!published);
    }

    #[test]
    fn relative_wikilink_resolution_prefers_relative_path_before_stem_fallback() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(vault.join("A")).expect("mkdir");

        fs::write(vault.join("B.md"), "---\npublish: true\n---\nRoot B\n").expect("write root b");
        fs::write(vault.join("A/B.md"), "---\npublish: true\n---\nNested B\n")
            .expect("write nested b");
        fs::write(
            vault.join("A/Note.md"),
            "---\npublish: true\n---\n[[../B]]\n[[B]]\n",
        )
        .expect("write note");

        run_publisher(runtime(&vault, &out)).expect("publish success");
        let note = fs::read_to_string(out.join("content/A/Note.md")).expect("note exists");
        assert!(note.contains("[B](/b/)"));
        assert!(note.contains("[B](/a/b/)"));
    }

    #[test]
    fn stem_disambiguation_prefers_shortest_relative_path_distance() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(vault.join("A/B/C")).expect("mkdir");

        fs::write(
            vault.join("Target.md"),
            "---\npublish: true\n---\nRoot target\n",
        )
        .expect("write root target");
        fs::write(
            vault.join("A/B/Target.md"),
            "---\npublish: true\n---\nNearby target\n",
        )
        .expect("write nearby target");
        fs::write(
            vault.join("A/B/C/Note.md"),
            "---\npublish: true\n---\n[[Target]]\n",
        )
        .expect("write note");

        run_publisher(runtime(&vault, &out)).expect("publish success");
        let note = fs::read_to_string(out.join("content/A/B/C/Note.md")).expect("note exists");
        assert!(note.contains("[Target](/a/b/target/)"));
        assert!(!note.contains("[Target](/target/)"));
    }

    #[test]
    fn asset_basename_resolution_supports_unique_and_ambiguous_cases() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(vault.join("assets")).expect("mkdir");
        fs::create_dir_all(vault.join("notes")).expect("mkdir");

        fs::write(vault.join("diagram.png"), "binary").expect("write root image");
        fs::write(
            vault.join("Unique.md"),
            "---\npublish: true\n---\n![[diagram.png]]\n![img](diagram.png)\n[asset](diagram.png)\n",
        )
        .expect("write unique note");

        run_publisher(runtime(&vault, &out)).expect("publish unique");
        let unique = fs::read_to_string(out.join("content/Unique.md")).expect("unique output");
        assert!(unique.contains("![](/assets/diagram.png)"));
        assert!(unique.contains("![img](/assets/diagram.png)"));
        assert!(unique.contains("[asset](/assets/diagram.png)"));

        fs::write(vault.join("assets/diagram.png"), "binary-2").expect("write duplicate image");
        fs::write(
            vault.join("notes/Ambiguous.md"),
            "---\npublish: true\n---\n![[diagram.png]]\n![img](diagram.png)\n[asset](diagram.png)\n",
        )
        .expect("write ambiguous note");

        run_publisher(runtime(&vault, &out)).expect("publish ambiguous");
        let ambiguous =
            fs::read_to_string(out.join("content/notes/Ambiguous.md")).expect("ambiguous output");
        assert!(ambiguous.contains("Ambiguous asset: diagram.png"));
        assert!(ambiguous.contains("![img](diagram.png)"));
        assert!(ambiguous.contains("[asset](diagram.png)"));
    }

    #[test]
    fn markdown_md_links_with_heading_fragments_rewrite_to_slugged_internal_urls() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(&vault).expect("mkdir");

        fs::write(
            vault.join("Home.md"),
            "---\npublish: true\n---\n[go](Target.md#My Heading)\n",
        )
        .expect("write home");
        fs::write(
            vault.join("Target.md"),
            "---\npublish: true\n---\n# My Heading\nbody\n",
        )
        .expect("write target");

        run_publisher(runtime(&vault, &out)).expect("publish success");
        let home = fs::read_to_string(out.join("content/Home.md")).expect("home output");
        assert!(home.contains("[go](/target/#my-heading)"));
    }

    #[test]
    fn dataview_preserve_to_frontmatter_moves_fields_and_strips_lines() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(&vault).expect("mkdir");

        fs::write(
            vault.join("Data.md"),
            "---\npublish: true\n---\nstatus:: open\nBody\n",
        )
        .expect("write note");

        let mut rt = runtime(&vault, &out);
        rt.app.dataview.mode = DataviewMode::PreserveToFrontmatter;
        rt.app.dataview.strip_inline_fields = None;

        run_publisher(rt).expect("publish success");
        let out_note = fs::read_to_string(out.join("content/Data.md")).expect("data output");
        assert!(out_note.contains("status = \"open\""));
        assert!(!out_note.contains("status:: open"));
    }

    #[test]
    fn dataview_preserve_to_frontmatter_extracts_inline_fields() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(&vault).expect("mkdir");

        fs::write(
            vault.join("Inline.md"),
            "---\npublish: true\n---\nTask status:: open\nBody\n",
        )
        .expect("write note");

        let mut rt = runtime(&vault, &out);
        rt.app.dataview.mode = DataviewMode::PreserveToFrontmatter;
        rt.app.dataview.strip_inline_fields = None;

        run_publisher(rt).expect("publish success");
        let out_note = fs::read_to_string(out.join("content/Inline.md")).expect("inline output");
        assert!(out_note.contains("status = \"open\""));
        assert!(!out_note.contains("status:: open"));
        assert!(out_note.contains("Task"));
    }

    #[test]
    fn dataview_legacy_strip_inline_fields_false_preserves_lines() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(&vault).expect("mkdir");

        fs::write(
            vault.join("Legacy.md"),
            "---\npublish: true\n---\nstatus:: open\nBody\n",
        )
        .expect("write note");

        let mut rt = runtime(&vault, &out);
        rt.app.dataview.mode = DataviewMode::Strip;
        rt.app.dataview.strip_inline_fields = Some(false);

        run_publisher(rt).expect("publish success");
        let out_note = fs::read_to_string(out.join("content/Legacy.md")).expect("legacy output");
        assert!(out_note.contains("status:: open"));
    }

    #[test]
    fn markdown_soft_line_break_hard_mode_converts_between_nonempty_lines() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(&vault).expect("mkdir");

        fs::write(
            vault.join("Breaks.md"),
            "---\npublish: true\n---\nfirst\nsecond\n\nthird\nfourth\n",
        )
        .expect("write note");

        let mut rt = runtime(&vault, &out);
        rt.app.markdown.soft_line_breaks = SoftLineBreakMode::HardBreaks;

        run_publisher(rt).expect("publish success");
        let note = fs::read_to_string(out.join("content/Breaks.md")).expect("breaks output");
        assert!(note.contains("first  \nsecond"));
        assert!(note.contains("third  \nfourth"));
    }

    #[test]
    fn frontmatter_always_emits_backlinks_array() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(&vault).expect("mkdir");

        fs::write(
            vault.join("Solo.md"),
            "---\npublish: true\n---\nJust text\n",
        )
        .expect("write solo");

        run_publisher(runtime(&vault, &out)).expect("publish success");
        let solo = fs::read_to_string(out.join("content/Solo.md")).expect("solo output");
        assert!(solo.contains("backlinks = []"));
    }

    #[test]
    fn invalid_metadata_dates_are_dropped_from_output() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(&vault).expect("mkdir");

        fs::write(
            vault.join("Dates.md"),
            "---\npublish: true\ndate: not-a-date\nupdated: 2026-99-99\n---\nBody\n",
        )
        .expect("write dated note");

        run_publisher(runtime(&vault, &out)).expect("publish success");
        let note = fs::read_to_string(out.join("content/Dates.md")).expect("dates output");
        assert!(!note.contains("date = "));
        assert!(!note.contains("updated = "));
    }

    #[test]
    fn incremental_run_prunes_deleted_outputs_without_full_cleanup() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(&vault).expect("mkdir");

        fs::write(vault.join("A.md"), "---\npublish: true\n---\nA\n").expect("write A");
        fs::write(vault.join("B.md"), "---\npublish: true\n---\nB\n").expect("write B");
        run_publisher(runtime(&vault, &out)).expect("initial publish");

        fs::write(out.join("content/keep.txt"), "keep").expect("write sentinel");
        fs::remove_file(vault.join("A.md")).expect("remove A");
        fs::write(vault.join("B.md"), "---\npublish: true\n---\nB2\n").expect("update B");

        let changed_paths = vec![vault.join("A.md"), vault.join("B.md")];
        run_publisher_incremental(runtime(&vault, &out), &changed_paths)
            .expect("incremental publish");

        assert!(!out.join("content/A.md").exists());
        assert!(out.join("content/B.md").exists());
        let b = fs::read_to_string(out.join("content/B.md")).expect("B output");
        assert!(b.contains("B2"));
        assert!(out.join("content/keep.txt").exists());
    }

    #[test]
    fn incremental_run_rewrites_backlink_dependents_without_touching_unrelated_notes() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(&vault).expect("mkdir");

        fs::write(
            vault.join("A.md"),
            "---\npublish: true\ntitle: A\n---\n[[B]]\n",
        )
        .expect("write A");
        fs::write(
            vault.join("B.md"),
            "---\npublish: true\ntitle: B\n---\nB body\n",
        )
        .expect("write B");
        fs::write(
            vault.join("C.md"),
            "---\npublish: true\ntitle: C\n---\nC body\n",
        )
        .expect("write C");

        run_publisher(runtime(&vault, &out)).expect("initial publish");

        let b_output = out.join("content/B.md");
        let c_output = out.join("content/C.md");
        let b_with_sentinel = format!(
            "{}\nSENTINEL_B\n",
            fs::read_to_string(&b_output).expect("read B output")
        );
        fs::write(&b_output, b_with_sentinel).expect("append sentinel to B");
        let c_with_sentinel = format!(
            "{}\nSENTINEL_C\n",
            fs::read_to_string(&c_output).expect("read C output")
        );
        fs::write(&c_output, c_with_sentinel).expect("append sentinel to C");

        fs::write(
            vault.join("A.md"),
            "---\npublish: true\ntitle: A2\n---\n[[B]]\n",
        )
        .expect("update A");

        let changed_paths = vec![vault.join("A.md")];
        run_publisher_incremental(runtime(&vault, &out), &changed_paths)
            .expect("incremental publish");

        let b_after = fs::read_to_string(&b_output).expect("read B after");
        let c_after = fs::read_to_string(&c_output).expect("read C after");

        assert!(!b_after.contains("SENTINEL_B"));
        assert!(b_after.contains("title = \"A2\""));
        assert!(b_after.contains("path = \"/a/\""));
        assert!(c_after.contains("SENTINEL_C"));
    }

    #[test]
    fn yaml_frontmatter_does_not_double_document_markers() {
        let metadata = ParsedMetadata {
            title: Some("Test".to_string()),
            ..Default::default()
        };
        let rendered = render_note_with_frontmatter(
            "body text",
            &metadata,
            "Test",
            "test",
            None,
            FrontmatterOutput::Yaml,
        )
        .expect("render yaml");

        // Should start with exactly one "---" fence, not two.
        assert!(rendered.starts_with("---\n"));
        assert!(!rendered.starts_with("---\n---\n"));
        assert!(rendered.contains("title: Test"));
        assert!(rendered.contains("body text"));
    }

    #[test]
    fn link_resolution_prefers_published_over_unpublished() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(vault.join("nearby")).expect("mkdir");

        // Nearby unpublished note with same stem
        fs::write(
            vault.join("nearby/Target.md"),
            "---\npublish: false\n---\nUnpublished\n",
        )
        .expect("write unpublished");

        // Far-away published note with same stem
        fs::write(
            vault.join("Target.md"),
            "---\npublish: true\n---\nPublished\n",
        )
        .expect("write published");

        // Note linking to "Target" — should resolve to the published one
        fs::write(
            vault.join("nearby/Note.md"),
            "---\npublish: true\n---\n[[Target]]\n",
        )
        .expect("write note");

        run_publisher(runtime(&vault, &out)).expect("publish success");
        let note = fs::read_to_string(out.join("content/nearby/Note.md")).expect("note output");
        // Should link to the published /target/, not show a broken-link span
        assert!(note.contains("[Target](/target/)"));
        assert!(!note.contains("broken-link"));
    }

    #[test]
    fn incremental_prunes_stale_section_index_after_last_note_deleted() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(vault.join("lonely")).expect("mkdir");

        // Single note in a subfolder
        fs::write(
            vault.join("lonely/Only.md"),
            "---\npublish: true\n---\nOnly note\n",
        )
        .expect("write only");

        run_publisher(runtime(&vault, &out)).expect("initial publish");
        assert!(out.join("content/lonely/_index.md").exists());

        // Delete the only note in that folder
        fs::remove_file(vault.join("lonely/Only.md")).expect("remove only");

        let changed_paths = vec![vault.join("lonely/Only.md")];
        run_publisher_incremental(runtime(&vault, &out), &changed_paths)
            .expect("incremental publish");

        // The orphan _index.md should be pruned
        assert!(
            !out.join("content/lonely/_index.md").exists(),
            "stale _index.md should be removed after last note deleted"
        );
    }

    #[test]
    fn publisher_rejects_nonexistent_output_inside_vault() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        fs::create_dir_all(&vault).expect("mkdir");

        // output_root is inside the vault but does not exist yet
        let out = vault.join("site");
        let reporter = Reporter::new(0, true);

        let result = Publisher::new(runtime(&vault, &out), reporter);
        assert!(
            result.is_err(),
            "Publisher::new should reject output inside vault even when it doesn't exist"
        );
    }

    #[test]
    fn dry_run_report_lines_include_per_file_notes_and_assets() {
        let tmp = TempDir::new().expect("tempdir");
        let vault = tmp.path().join("vault");
        let out = tmp.path().join("site");
        fs::create_dir_all(&vault).expect("mkdir");

        let publisher =
            Publisher::new(runtime(&vault, &out), Reporter::new(0, true)).expect("publisher");
        let processed = vec![ProcessedNote {
            note_id: 0,
            output_relative_path: PathBuf::from("Notes/Home.md"),
            content: String::new(),
            outbound_ids: HashSet::new(),
            metadata: ParsedMetadata::default(),
        }];
        let assets = vec![PathBuf::from("images/pic.png")];

        let lines = publisher.build_dry_run_report_lines(&processed, &assets);

        assert!(lines.iter().any(
            |line| line == "dry-run: would process note Notes/Home.md -> content/Notes/Home.md"
        ));
        assert!(lines.iter().any(|line| line
            == "dry-run: would process asset images/pic.png -> static/assets/images/pic.png"));
    }
}
