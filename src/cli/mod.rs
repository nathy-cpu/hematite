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
