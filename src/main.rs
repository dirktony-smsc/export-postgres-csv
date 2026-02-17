fn main() {
    if let Err(err) = export_db_csv::run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
