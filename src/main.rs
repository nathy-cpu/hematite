use hematite::{script_is_complete, ExecutedStatement, Hematite, HematiteError};
use std::env;
use std::io::{self, Write};

enum CliMode {
    Usage,
    Interactive { db_path: String },
    OneShot { db_path: String, script: String },
}

fn main() -> Result<(), HematiteError> {
    match parse_cli_args(env::args().skip(1)) {
        CliMode::Usage => {
            print_usage();
            std::process::exit(1);
        }
        CliMode::Interactive { db_path } => run_interactive(&db_path),
        CliMode::OneShot { db_path, script } => run_one_shot(&db_path, &script),
    }
}

fn parse_cli_args(args: impl IntoIterator<Item = String>) -> CliMode {
    let args = args.into_iter().collect::<Vec<_>>();
    match args.as_slice() {
        [] => CliMode::Usage,
        [db_path] => CliMode::Interactive {
            db_path: db_path.clone(),
        },
        [db_path, sql @ ..] => CliMode::OneShot {
            db_path: db_path.clone(),
            script: sql.join(" "),
        },
    }
}

fn print_usage() {
    eprintln!("Usage: hematite <db_path> [sql ...]");
    eprintln!("  hematite <db_path>          Start interactive SQL mode");
    eprintln!("  hematite <db_path> <sql>    Execute one SQL script and exit");
}

fn run_one_shot(db_path: &str, script: &str) -> Result<(), HematiteError> {
    let mut db = Hematite::new(db_path)?;
    for result in db.iter_script(script)? {
        print_execution_result(result?)?;
    }
    Ok(())
}

fn run_interactive(db_path: &str) -> Result<(), HematiteError> {
    let mut db = Hematite::new(db_path)?;
    let mut buffer = String::new();

    println!("Hematite Database CLI");
    println!("Type SQL statements ending with ';' or .exit to quit");

    loop {
        let prompt = if buffer.trim().is_empty() {
            "db > "
        } else {
            "... > "
        };

        print!("{prompt}");
        io::stdout().flush()?;

        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            println!();
            break;
        }

        let trimmed = input.trim();
        if buffer.trim().is_empty() && matches!(trimmed, ".exit" | ".quit") {
            println!("Bye!");
            break;
        }

        if trimmed.is_empty() {
            continue;
        }

        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(trimmed);

        if !script_is_complete(&buffer)? {
            continue;
        }

        match execute_script(&mut db, &buffer) {
            Ok(()) => buffer.clear(),
            Err(err) => {
                eprintln!("Error: {err}");
                buffer.clear();
            }
        }
    }

    Ok(())
}

fn execute_script(db: &mut Hematite, script: &str) -> Result<(), HematiteError> {
    for result in db.iter_script(script)? {
        print_execution_result(result?)?;
    }
    Ok(())
}

fn print_execution_result(result: ExecutedStatement) -> Result<(), HematiteError> {
    match result {
        ExecutedStatement::Statement(statement) => {
            println!("{} ({})", statement.message, statement.affected_rows);
        }
        ExecutedStatement::Query(result_set) => {
            if !result_set.columns.is_empty() {
                println!("{}", result_set.columns.join(" | "));
            }
            for row in result_set.iter() {
                let values = row
                    .values
                    .iter()
                    .map(|value| format!("{value:?}"))
                    .collect::<Vec<_>>();
                println!("{}", values.join(" | "));
            }
            println!("{} row(s)", result_set.len());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
}
