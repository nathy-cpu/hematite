use hematite::{query::QueryResult, Connection, HematiteError};
use std::io::Write;

fn main() -> Result<(), HematiteError> {
    let mut db = Connection::new("test.db")?;

    println!("Hematite Database CLI");
    println!("Type SQL commands or .exit to quit");

    loop {
        let mut input = String::new();
        print!("db > ");
        std::io::stdout().flush()?;

        std::io::stdin().read_line(&mut input)?;
        input = input.trim().to_string();

        if input == ".exit" {
            println!("Bye!");
            break;
        }

        if input.is_empty() {
            continue;
        }

        // Parse and execute SQL commands
        match execute_sql_command(&mut db, &input) {
            Ok(result) => {
                println!("✓ Ok");
                if !result.columns.is_empty() {
                    // This was a SELECT query - show results
                    println!("Columns: {:?}", result.columns);
                    for row in &result.rows {
                        println!("Row: {:?}", row);
                    }
                    println!("{} rows returned", result.rows.len());
                } else {
                    // This was a DML statement
                    println!("{} rows affected", result.rows.len());
                }
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }

    db.close()?;
    Ok(())
}

fn execute_sql_command(db: &mut Connection, sql: &str) -> Result<QueryResult, HematiteError> {
    db.execute(sql)
}
