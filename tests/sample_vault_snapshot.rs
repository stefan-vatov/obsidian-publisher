use obsidian_publisher::config::{AppConfig, RuntimeConfig};
use obsidian_publisher::preprocess::run_publisher;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use walkdir::WalkDir;

fn collect_relative_files(root: &Path) -> BTreeSet<PathBuf> {
    WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter_map(|entry| entry.path().strip_prefix(root).ok().map(Path::to_path_buf))
        .collect()
}

#[test]
fn sample_vault_matches_expected_snapshot() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let vault_root = manifest_dir.join("tests/fixtures/sample_vault");
    let expected_root = manifest_dir.join("tests/fixtures/sample_vault_expected");
    let tmp = TempDir::new().expect("tempdir");
    let output_root = tmp.path().join("site");

    let runtime = RuntimeConfig {
        vault_root: vault_root.clone(),
        output_root: output_root.clone(),
        app: AppConfig::default(),
        dry_run: false,
        watch: false,
        verbose: 0,
        quiet: true,
    };

    run_publisher(runtime).expect("sample-vault publish succeeds");

    for scope in ["content", "static"] {
        let expected_scope = expected_root.join(scope);
        let actual_scope = output_root.join(scope);
        let expected_files = collect_relative_files(&expected_scope);
        let actual_files = collect_relative_files(&actual_scope);

        assert_eq!(
            actual_files, expected_files,
            "file list mismatch for scope '{scope}'"
        );

        for rel in expected_files {
            let expected = fs::read(expected_scope.join(&rel))
                .unwrap_or_else(|err| panic!("failed to read expected {}: {err}", rel.display()));
            let actual = fs::read(actual_scope.join(&rel))
                .unwrap_or_else(|err| panic!("failed to read actual {}: {err}", rel.display()));
            assert_eq!(actual, expected, "content mismatch for {}", rel.display());
        }
    }
}
