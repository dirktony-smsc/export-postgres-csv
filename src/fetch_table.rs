use csv::StringRecord;
use postgres::{Client, Statement};
use postgres_types::Type;

#[derive(derive_more::Debug)]
pub struct FetchTableData {
    current: Vec<StringRecord>,
    fetch_statement: Statement,
    limit: i64,
    total: i64,
    offset: i64,
    column_names: Vec<String>,
    #[debug(ignore)]
    pool: crate::PooledConnection,
}

impl FetchTableData {
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }
    #[allow(unused)]
    pub fn get_current_ref(&self) -> &[StringRecord] {
        &self.current
    }
    pub fn take_current(&mut self) -> Vec<StringRecord> {
        std::mem::take(&mut self.current)
    }
    pub fn new(pool: crate::PoolConnection, table_name: &str) -> anyhow::Result<Self> {
        let mut connection = pool.get()?;
        let column_names = get_table_column_name(&mut connection, table_name)?;
        let fetch_statement = {
            let column_name_aggregated = {
                use std::fmt::Write;
                let mut column_name_aggregated = String::new();
                for name in &column_names {
                    write!(
                        &mut column_name_aggregated,
                        "coalesce(\"{name}\"::text, '') as \"{name}\", "
                    )?;
                }
                write!(&mut column_name_aggregated, "null as \"________nothing\"")?;
                column_name_aggregated
            };
            let query_inner = format!("SELECT {column_name_aggregated} from \"{table_name}\"");
            log::debug!("query_inner: {query_inner}");
            let query_to_prepare = format!(
                "SELECT *, COUNT(*) OVER () as \"__table_count_total\" FROM ({query_inner}) as paged_query_with OFFSET $1 LIMIT $2"
            );
            log::debug!("to preprare: {query_to_prepare}");
            connection.prepare_typed(&query_to_prepare, &[Type::INT8, Type::INT8])?
        };
        let offset = 0i64;
        let limit = crate::LIMIT_FETCH as i64;
        let rows = connection.query(&fetch_statement, &[&offset, &limit])?;
        let total = rows
            .first()
            .map(|d| d.get::<_, i64>("__table_count_total"))
            .unwrap_or_default();
        let current = rows
            .into_iter()
            .map(|row| {
                let mut record = StringRecord::new();
                for name in &column_names {
                    record.push_field(&row.try_get::<_, String>(name.as_str())?);
                }
                Ok::<_, postgres::Error>(record)
            })
            .collect::<Result<Vec<_>, postgres::Error>>()?;
        Ok(Self {
            current,
            fetch_statement,
            limit,
            total,
            offset,
            column_names,
            pool: connection,
        })
    }
    /// Return [`true`] if we pulled some data, [`false`] otherwise
    pub fn next_in_place(&mut self) -> anyhow::Result<bool> {
        let offset = self.offset + self.limit;
        if self.total < offset {
            self.current.clear();
            return Ok(false);
        }
        let connection = &mut self.pool;
        let rows = connection.query(&self.fetch_statement, &[&offset, &self.limit])?;
        let total = rows
            .first()
            .map(|d| d.get::<_, i64>("__table_count_total"))
            .unwrap_or_default();
        let current = rows
            .into_iter()
            .map(|row| {
                let mut record = StringRecord::new();
                for name in self.column_names() {
                    record.push_field(row.try_get(name.as_str())?);
                }
                Ok::<_, postgres::Error>(record)
            })
            .collect::<Result<Vec<_>, postgres::Error>>()?;
        self.offset = offset;
        self.current = current;
        self.total = total;
        Ok(true)
    }
    pub fn get_csv_header(&self) -> StringRecord {
        let mut header = StringRecord::new();
        for name in &self.column_names {
            header.push_field(name);
        }
        header
    }
}

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
