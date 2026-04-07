use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

fn collect_rs_files(root: &Path, files: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rs_files(&path, files);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }
}

fn top_level_module(path: &Path) -> Option<&'static str> {
    let path = path.strip_prefix("src").ok()?;
    let first = path.iter().next()?.to_str()?;
    match first {
        "storage" => Some("storage"),
        "btree" => Some("btree"),
        "catalog" => Some("catalog"),
        "query" => Some("query"),
        "parser" => Some("parser"),
        "sql" => Some("sql"),
        "main.rs" => Some("main"),
        _ => None,
    }
}

fn is_production_file(path: &Path) -> bool {
    let file_name = path.file_name().and_then(|name| name.to_str());
    match file_name {
        Some("tests.rs") => false,
        Some(name) if name.ends_with("_test.rs") => false,
        _ => true,
    }
}

fn extract_crate_import_modules(contents: &str) -> BTreeSet<String> {
    let mut modules = BTreeSet::new();

    for raw_line in contents.lines() {
        let line = raw_line.trim_start();
        let prefix = if let Some(rest) = line.strip_prefix("use crate::") {
            rest
        } else if let Some(rest) = line.strip_prefix("pub use crate::") {
            rest
        } else {
            continue;
        };

        let module = prefix
            .split(|ch: char| [';', ':', '{', ',', ' ', '('].contains(&ch))
            .next()
            .unwrap_or_default()
            .trim();

        if matches!(module, "Result" | "HematiteError") {
            modules.insert("error".to_string());
        } else if !module.is_empty() {
            modules.insert(module.to_string());
        }
    }

    modules
}

#[test]
fn production_imports_match_layer_matrix_or_temporary_exceptions() {
    let allowed: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::from([
        ("storage", BTreeSet::from(["storage", "error"])),
        ("btree", BTreeSet::from(["btree", "storage", "error"])),
        ("catalog", BTreeSet::from(["catalog", "btree", "error"])),
        (
            "query",
            BTreeSet::from(["query", "parser", "catalog", "error"]),
        ),
        ("parser", BTreeSet::from(["parser", "error"])),
        ("sql", BTreeSet::from(["sql", "query", "parser", "error"])),
        ("main", BTreeSet::from(["sql", "error"])),
    ]);

    let temporary_exceptions: BTreeSet<(&str, &str)> = BTreeSet::new();

    let mut files = Vec::new();
    collect_rs_files(Path::new("src"), &mut files);
    files.sort();

    let mut violations = Vec::new();

    for path in files {
        if !is_production_file(&path) {
            continue;
        }

        let Some(module) = top_level_module(&path) else {
            continue;
        };
        let Some(allowed_modules) = allowed.get(module) else {
            continue;
        };

        let contents = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {}", path.display(), err));
        let imported_modules = extract_crate_import_modules(&contents);
        let path_string = path.to_string_lossy().to_string();

        for imported_module in imported_modules {
            if allowed_modules.contains(imported_module.as_str()) {
                continue;
            }
            if temporary_exceptions.contains(&(path_string.as_str(), imported_module.as_str())) {
                continue;
            }
            violations.push(format!(
                "{} imports crate::{} but module '{}' only allows {:?}",
                path_string, imported_module, module, allowed_modules
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "unexpected architecture boundary violations:\n{}",
        violations.join("\n")
    );
}
