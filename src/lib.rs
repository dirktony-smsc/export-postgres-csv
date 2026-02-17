pub(crate) mod all_tables;
pub(crate) mod fetch_table;
pub(crate) mod utils;

use std::{
    fs::{File, create_dir_all},
    num::NonZeroU32,
};

use clap::Parser;
use csv::WriterBuilder;
use indicatif::{MultiProgress, ProgressBar};
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

    //workers: Option<u16>,
    /// the table owner to get table on
    ///
    /// Default to [`Self::user`] if not set
    #[arg(long)]
    table_owner: Option<String>,
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
    fn table_owner(&self) -> &str {
        self.table_owner.as_deref().unwrap_or(&self.user)
    }
}

pub(crate) type PoolConnection = r2d2::Pool<PostgresConnectionManager<NoTls>>;

pub fn run_with_progress(multi_progress: MultiProgress) -> anyhow::Result<()> {
    let cli = Cli::parse();

    let con = cli.pool()?;
    let all_tabls = {
        let owner = cli.table_owner();
        let fetching =
            ProgressBar::new_spinner().with_message(format!("Fetching all tables of {owner}"));
        let mut tab_names = Vec::<String>::new();
        let mut all_tabls = all_tables::AllTableNameStream::new(con.clone(), owner)?;
        loop {
            tab_names.extend(all_tabls.take_current());
            if !all_tabls.next_in_place()? {
                break;
            }
        }
        fetching.finish_with_message(format!("Got {} tables", tab_names.len()));
        tab_names
    };
    let dir = {
        create_dir_all(&cli.directory)?;
        std::path::Path::new(&cli.directory)
            .to_path_buf()
            .canonicalize()?
    };
    {
        use indicatif::ProgressIterator;
        let progress_iter = all_tabls.into_iter().progress();
        let progress = progress_iter.progress.clone();
        multi_progress.add(progress.clone());
        for table in progress_iter {
            let mut table_fetch = fetch_table::FetchTableData::new(con.clone(), &table)?;
            let mut file =
                WriterBuilder::new().from_writer(File::create(dir.join(format!("{table}.csv")))?);
            file.write_byte_record(table_fetch.get_csv_header().as_byte_record())?;
            loop {
                for records in table_fetch.take_current() {
                    file.write_byte_record(records.as_byte_record())?;
                }
                if !table_fetch.next_in_place()? {
                    break;
                }
            }
            file.flush()?;
        }
        progress.finish_with_message(format!(
            "Exported all tables as csv to {}",
            dir.to_str()
                .ok_or(anyhow::anyhow!("Cannot convert PathBuf to str"))?
        ));
    }
    Ok(())
}

pub fn run() {
    let multi_progress = MultiProgress::new();
    if let Err(err) = run_with_progress(multi_progress.clone()) {
        let _ = multi_progress.clear();
        eprintln!("{err}");
        std::process::exit(1)
    }
}
