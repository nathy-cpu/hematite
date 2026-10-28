//! Argument handling for `hematite_cli`.
//!
//! One argument opens the database interactively. More than one runs the rest as a
//! single SQL script and exits; they are joined with spaces, so a shell that splits an
//! unquoted statement into words still passes it through whole.

pub(crate) enum CliMode {
    Usage,
    Interactive { db_path: String },
    OneShot { db_path: String, script: String },
}

pub(crate) fn parse_cli_args(args: impl IntoIterator<Item = String>) -> CliMode {
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

#[cfg(test)]
mod tests;
