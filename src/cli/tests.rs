use super::{parse_cli_args, CliMode};

#[test]
fn test_parse_cli_args_usage() {
    assert!(matches!(
        parse_cli_args(Vec::<String>::new()),
        CliMode::Usage
    ));
}

#[test]
fn test_parse_cli_args_interactive() {
    match parse_cli_args(vec!["demo.db".to_string()]) {
        CliMode::Interactive { db_path } => assert_eq!(db_path, "demo.db"),
        CliMode::Usage | CliMode::OneShot { .. } => panic!("expected interactive mode"),
    }
}

#[test]
fn test_parse_cli_args_one_shot() {
    match parse_cli_args(vec![
        "demo.db".to_string(),
        "SELECT".to_string(),
        "1;".to_string(),
    ]) {
        CliMode::OneShot { db_path, script } => {
            assert_eq!(db_path, "demo.db");
            assert_eq!(script, "SELECT 1;");
        }
        CliMode::Usage | CliMode::Interactive { .. } => panic!("expected one-shot mode"),
    }
}
