mod cli;

use crate::cli::{parse_cli_args, CliMode};
use hematite::{script_is_complete, Hematite, HematiteError};
use std::env;
use std::io::{self, Write};

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

fn print_usage() {
    eprintln!("Usage: hematite <db_path> [sql ...]");
    eprintln!("  hematite <db_path>          Start interactive SQL mode");
    eprintln!("  hematite <db_path> <sql>    Execute one SQL script and exit");
}

fn run_one_shot(db_path: &str, script: &str) -> Result<(), HematiteError> {
    let mut db = Hematite::new(db_path)?;
    for result in db.iter_script(script)? {
        println!("{}", result?.render_ascii());
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
        println!("{}", result?.render_ascii());
    }
    Ok(())
}
