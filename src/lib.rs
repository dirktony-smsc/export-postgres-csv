pub(crate) mod all_tables;
pub(crate) mod export;
pub(crate) mod fetch_table;
pub(crate) mod utils;

use std::{num::NonZeroU32, time::SystemTime};

use clap::{Parser, Subcommand};
use fern::{Dispatch, colors::ColoredLevelConfig};
use indicatif::MultiProgress;
use postgres::{Config as PostgresCfg, NoTls};
use r2d2_postgres::PostgresConnectionManager;

use crate::export::ExportTablesToDir;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
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
    /// Database name
    #[arg(short)]
    database: String,
    /// Sets the maximum number of connections managed by the pool.
    #[arg(long)]
    max_connection: Option<NonZeroU32>,
    /// verbose...
    #[arg(short)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Export(export::ExportArgs),
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

pub fn run_with_progress(multi_progress: MultiProgress) -> anyhow::Result<()> {
    let cli = Cli::parse();

    let con = cli.pool()?;
    if cli.verbose {
        let colors = ColoredLevelConfig::new();
        Dispatch::new()
            .format(move |out, msg, record| {
                out.finish(format_args!(
                    "[{} {} {}] {}",
                    humantime::format_rfc3339_seconds(SystemTime::now()),
                    colors.color(record.level()),
                    record.target(),
                    msg
                ));
            })
            .level(log::LevelFilter::Debug)
            .chain({
                let multi = multi_progress.clone();
                fern::Output::call(move |rec| {
                    multi.suspend(|| {
                        println!("{}", rec.args());
                    })
                })
            })
            .apply()?;
    }
    match &cli.command {
        Commands::Export(export_args) => ExportTablesToDir {
            pool: con,
            progress: multi_progress,
            table_owner: export_args.table_owner.clone().unwrap_or(cli.user.clone()),
            directory: export_args.directory.clone(),
            parallel: export_args.parallel,
        }
        .run()?,
    }
    Ok(())
}

pub fn run() {
    let multi_progress = MultiProgress::new();
    if let Err(err) = run_with_progress(multi_progress.clone()) {
        let _ = multi_progress.clear();
        eprintln!("{:?}", err);
        std::process::exit(1)
    }
}
