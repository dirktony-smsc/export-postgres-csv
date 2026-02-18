use postgres::Client;

pub fn get_table_column_name(
    connection: &mut Client,
    table: &str,
) -> Result<Vec<String>, postgres::Error> {
    connection
        .query(
            "SELECT column_name from information_schema.columns where table_name = $1",
            &[&table],
        )?
        .into_iter()
        .map(|row| row.try_get("column_name"))
        .collect()
}
