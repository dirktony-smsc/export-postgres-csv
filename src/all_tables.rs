use postgres::{Client, Statement, types::Type};

pub struct AllTableNameStream {
    limit: u32,
    offset: u32,
    pool: crate::PooledConnection,
    current: Vec<String>,
    total: u64,
    fetch_statement: Statement,
}

impl AllTableNameStream {
    #[allow(unused)]
    pub fn get_current_ref(&self) -> &[String] {
        &self.current
    }
    pub fn take_current(&mut self) -> Vec<String> {
        std::mem::take(&mut self.current)
    }
    pub fn new(pool: crate::PoolConnection, owner: &str) -> anyhow::Result<Self> {
        let mut connection = pool.get()?;
        let total = table_count(&mut connection, owner)?;
        let fetch_statement = connection.prepare_typed(
            &format!(
                "SELECT * FROM pg_catalog.pg_tables where tableowner = '{}' offset $1 limit $2",
                owner
            ),
            &[Type::OID, Type::OID],
        )?;
        let offset = 0u32;
        let limit = crate::LIMIT_FETCH;
        let rows = connection
            .query(&fetch_statement, &[&offset, &limit])?
            .into_iter()
            .map(|row| row.try_get::<_, String>("tablename"))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            fetch_statement,
            limit,
            offset,
            total,
            current: rows,
            pool: connection,
        })
    }
    /// Return [`true`] if we pulled some data, [`false`] otherwise
    pub fn next_in_place(&mut self) -> anyhow::Result<bool> {
        let offset = self.offset + self.limit;
        if self.total < (offset as _) {
            self.current.clear();
            return Ok(false);
        }
        let rows = self
            .pool
            .query(&self.fetch_statement, &[&offset, &self.limit])?
            .into_iter()
            .map(|row| {
                row.try_get::<_, String>("tablename")
                    .inspect_err(|e| eprintln!("got conversion error {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.offset = offset;
        self.current = rows;
        Ok(true)
    }
}

fn table_count(client: &mut Client, owner: &str) -> Result<u64, anyhow::Error> {
    let statement = client.prepare_typed(
        "SELECT COUNT(*) as tables FROM pg_catalog.pg_tables where tableowner = $1",
        &[Type::VARCHAR],
    )?;
    let tables: i64 = client.query_one(&statement, &[&owner])?.try_get("tables")?;

    Ok(tables.try_into()?)
}

#[cfg(test)]
mod test {
    use std::{
        fs::File,
        io::{BufWriter, Write},
    };

    use crate::all_tables::AllTableNameStream;

    #[test]
    fn get_all_tables_name() -> anyhow::Result<()> {
        let pool = crate::utils::tests::get_pool_from_env();
        let mut tablenames = AllTableNameStream::new(pool, "tony")?;
        let mut file = BufWriter::new(File::create("target/all-tables.txt")?);
        loop {
            tablenames.current.iter().for_each(|name| {
                let _ = writeln!(&mut file, "{name}");
            });
            if !tablenames.next_in_place()? {
                break;
            }
        }
        file.flush()?;
        Ok(())
    }
}
