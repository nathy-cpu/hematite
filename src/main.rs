use hematite::{Database, HematiteError};
use std::io::Write;

fn main() -> Result<(), HematiteError> {
    let mut db = Database::open("test.db")?;

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

        // TODO: Parse and execute SQL commands
        println!("Command received: {}", input);
    }

    db.close()?;
    Ok(())
}
