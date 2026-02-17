pub(crate) mod all_tables;
pub(crate) mod utils;

use std::num::NonZeroU32;

use clap::Parser;
use postgres::{Config as PostgresCfg, NoTls};
use r2d2_postgres::PostgresConnectionManager;

#[derive(Debug, Parser)]
pub struct Cli {
    /// Postgres user
    #[arg(short)]
    user: String,
    /// Postgres password
    #[arg(short = 'w')]
    password: String,
    /// postgres host
    #[arg(long)]
    host: String,
    /// Postgres port
    #[arg(short)]
    port: Option<u16>,
    /// Sets the maximum number of connections managed by the pool.
    #[arg(long)]
    max_connection: Option<NonZeroU32>,
    /// Database name
    database: String,
    /// The directory to put the csv file to
    directory: String,
}

pub(crate) const LIMIT_FETCH: u32 = 100;

impl Cli {
    fn to_postgres_cfg(&self) -> PostgresCfg {
        let mut cfg = PostgresCfg::new();
        cfg.dbname(&self.database)
            .port(self.port.unwrap_or(5432))
            .host(&self.host)
            .user(&self.user)
            .password(&self.password);
        cfg
    }
    fn pool(&self) -> Result<PoolConnection, r2d2::Error> {
        let mut builder = r2d2::Builder::new();
        if let Some(max) = self.max_connection {
            builder = builder.max_size(max.into());
        }
        builder.build(r2d2_postgres::PostgresConnectionManager::new(
            self.to_postgres_cfg(),
            NoTls,
        ))
    }
}

pub(crate) type PoolConnection = r2d2::Pool<PostgresConnectionManager<NoTls>>;

pub fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let con = cli.pool()?;
    Ok(())
}
