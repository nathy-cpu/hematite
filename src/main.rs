use std::io::{self, Write};
use std::process::exit;

fn main() -> ! {
    let mut input = String::new();

    loop {
        let prompt = String::from("db > ");
        print(&prompt);

        read_input(&mut input);

        if input.as_bytes()[0] == b'.' {
            match handle_meta_command(&input) {
                MetaCommand::EXIT => {
                    println!("Bye");
                    exit(0);
                }
                MetaCommand::UNRECOGNISED => {
                    println!("Unrecognized command: {}", input);
                    continue;
                }
                MetaCommand::SUCCESS => continue,
            };
        }

        let mut statement = Statement { _type: StatementType::SELECT }; // just for the compiler
        match statement.prepare(&input) {
            PrepareResult::SUCCESS => {
                statement.execute();
                println!("Executed.");
                input.clear();
            },
            PrepareResult::UNRECOGNISED => {
                println!("Unrecognized keyword at start of: {}", input);
                continue;
            }
        }

    }
}


fn handle_meta_command(input: &String) -> MetaCommand {
    if input.starts_with(".exit") {
        return MetaCommand::EXIT;
    } else {
        return MetaCommand::UNRECOGNISED;
    }
}

fn print(output: &String) {
    io::stdout().write(output.as_bytes()).expect("Failed to write to output buffer");
    io::stdout().flush().expect("Failed to flush output buffer"); // force print
}

fn read_input(input: &mut String) {
    io::stdin().read_line(input).expect("Failed to read line");
    *input = input.trim().to_string();
}

enum MetaCommand {
    SUCCESS, UNRECOGNISED, EXIT
}

enum PrepareResult {
    SUCCESS, UNRECOGNISED
}

enum StatementType {
    SELECT, INSERT
}

struct Statement {
    _type: StatementType
}

impl Statement {
    fn prepare(&mut self, input: &String) -> PrepareResult {
        if input.starts_with("insert") {
            self._type = StatementType::INSERT;
            return PrepareResult::SUCCESS;
        } else if input == "select" {
            self._type = StatementType::SELECT;
            return PrepareResult::SUCCESS;
        }
        return PrepareResult::UNRECOGNISED;
    }

    fn execute(&self) {
        match self._type {
            StatementType::SELECT => println!("Assume this is where we selected :)"),
            StatementType::INSERT => println!("Assume this is where we inserted :)"),
        }
    }
}
