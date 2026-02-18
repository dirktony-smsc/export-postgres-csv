use std::{
    collections::VecDeque,
    fs::{File, create_dir_all},
    path::PathBuf,
};

use clap::Args;
use csv::WriterBuilder;
use indicatif::{MultiProgress, ProgressBar};
use rayon::iter::IntoParallelIterator;

#[derive(Debug, Args)]
pub struct ExportArgs {
    //workers: Option<u16>,
    /// the table owner to get table on
    ///
    /// Default to [`Self::user`] if not set
    #[arg(long)]
    pub table_owner: Option<String>,
    /// The directory to put the csv file to
    pub directory: String,
    /// Run data in parallel
    #[arg(short)]
    pub parallel: bool,
}

pub struct ExportTablesToDir {
    pub pool: crate::PoolConnection,
    pub table_owner: String,
    pub directory: String,
    pub progress: MultiProgress,
    pub parallel: bool,
}

impl ExportTablesToDir {
    fn export_table(
        table: String,
        pool: crate::PoolConnection,
        dir: PathBuf,
    ) -> anyhow::Result<()> {
        let mut table_fetch = crate::fetch_table::FetchTableData::new(pool.clone(), &table)?;
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
        Ok(())
    }
    pub fn run(self) -> anyhow::Result<()> {
        let all_tabls: VecDeque<_> = {
            let owner = self.table_owner;
            let fetching =
                ProgressBar::new_spinner().with_message(format!("Fetching all tables of {owner}"));
            self.progress.add(fetching.clone());
            let mut tab_names = Vec::<String>::new();
            let mut all_tabls =
                crate::all_tables::AllTableNameStream::new(self.pool.clone(), &owner)?;
            loop {
                tab_names.extend(all_tabls.take_current());
                if !all_tabls.next_in_place()? {
                    break;
                }
            }
            fetching.finish_with_message(format!("Got {} tables", tab_names.len()));
            tab_names.into()
        };
        let dir = {
            create_dir_all(&self.directory)?;
            std::path::Path::new(&self.directory)
                .to_path_buf()
                .canonicalize()?
        };
        if self.parallel {
            use indicatif::ParallelProgressIterator;
            use rayon::iter::ParallelIterator;
            let dir = dir.clone();
            let pool = self.pool.clone();
            let progress_par_iter = all_tabls.into_par_iter().progress();
            let progress = progress_par_iter.progress.clone();
            self.progress.add(progress.clone());
            progress_par_iter
                .map(move |table| Self::export_table(table, pool.clone(), dir.clone()))
                .collect::<anyhow::Result<()>>()?;
        } else {
            use indicatif::ProgressIterator;
            let progress_iter = all_tabls.into_iter().progress();
            let progress = progress_iter.progress.clone();
            self.progress.add(progress.clone());
            for table in progress_iter {
                Self::export_table(table, self.pool.clone(), dir.clone())?;
            }
        }
        println!(
            "Exported all tables as csv to {}",
            dir.to_str()
                .ok_or(anyhow::anyhow!("Cannot convert PathBuf to str"))?
        );
        Ok(())
    }
}
