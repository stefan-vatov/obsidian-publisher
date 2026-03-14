use std::fs;
use std::process::Command;

use tempfile::TempDir;

#[test]
fn dry_run_reports_per_file_notes_and_assets() {
    let tmp = TempDir::new().expect("tempdir");
    let vault = tmp.path().join("vault");
    let out = tmp.path().join("site");
    fs::create_dir_all(&vault).expect("mkdir");

    fs::write(vault.join("Note.md"), "---\npublish: true\n---\nBody\n").expect("write note");
    fs::write(vault.join("diagram.png"), "binary").expect("write asset");

    let binary = env!("CARGO_BIN_EXE_obsidian-publisher");
    let output = Command::new(binary)
        .arg("--vault")
        .arg(&vault)
        .arg("--output")
        .arg(&out)
        .arg("--dry-run")
        .output()
        .expect("run binary");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "stderr:\n{stderr}");
    assert!(
        stderr.contains("dry-run: would process note Note.md -> content/Note.md"),
        "stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("dry-run: would process asset diagram.png -> static/assets/diagram.png"),
        "stderr:\n{stderr}"
    );

    assert!(!out.exists());
}
