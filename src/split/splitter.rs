use std::{collections::HashMap, ffi::OsStr, path::PathBuf};

use anyhow::Result;
use async_zip::{
    base::{read::seek::ZipFileReader, write::ZipFileWriter},
    Compression, ZipEntryBuilder, ZipString,
};
use futures::{AsyncReadExt, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info, warn};
use tokio::{
    fs::File,
    io::{self, AsyncWrite, AsyncWriteExt, BufReader},
};
use tokio_util::{
    compat::{Compat, FuturesAsyncWriteCompatExt, TokioAsyncWriteCompatExt},
    io::ReaderStream,
};
use trauma::{download::Download, downloader::DownloaderBuilder};

use super::{model, Config};

const BUF_SIZE: usize = 65536;

static SHARED_NAMES: &[&str] = &[
    "users.json",
    "channels.json",
    "dms.json",
    "groups.json",
    "mpims.json",
];

pub struct Splitter {
    config: Config,
    reader: ZipFileReader<Compat<BufReader<File>>>,

    shared_files_idx: HashMap<String, usize>,
    // List of chunks with file path:idx mapping
    chunked_files_idx: Vec<HashMap<String, usize>>,

    pb: ProgressBar,
}

impl Splitter {
    pub async fn new(config: Config) -> Result<Self> {
        info!("Open zip file...");
        let pb = ProgressBar::new_spinner();
        let reader = ZipFileReader::with_tokio(BufReader::new(
            File::open(config.slack_archive.clone()).await?,
        ))
        .await?;
        Ok(Splitter {
            reader,
            config,
            pb,
            shared_files_idx: HashMap::new(),
            chunked_files_idx: Vec::new(),
        })
    }

    pub async fn split(&mut self) -> Result<()> {
        info!("Sptit..");
        self.scan_files().await?;
        self.export_chunks().await?;
        Ok(())
    }

    pub async fn scan_files(&mut self) -> Result<()> {
        info!("Scan zip file structure and split to chunks...");
        dbg!(&self.config);

        // Calculate chunk size
        let total_entries = self.reader.file().entries().len();

        let chunk_size = if self.config.num_chunks > 1 {
            total_entries / self.config.num_chunks
        } else {
            self.config.chunk_size
        };

        info!("Split {total_entries} files to chunks by {chunk_size} items maximum...");

        self.pb = ProgressBar::new(total_entries as u64);
        self.pb.set_style(
                ProgressStyle::with_template(
                    "Scan files: {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len}, ETA {eta})",
                )
                .unwrap(),
            );

        for (chunk_idx, chunk) in self.reader.file().entries().chunks(chunk_size).enumerate() {
            let mut chunk_map: HashMap<String, usize> = HashMap::new();
            for (entry_idx, entry) in chunk.iter().enumerate() {
                let index = chunk_size * chunk_idx + entry_idx;
                self.pb.inc(1);
                if let Ok(filename) = entry.filename().clone().into_string() {
                    if filename.ends_with(".json") && !SHARED_NAMES.contains(&filename.as_ref()) {
                        chunk_map.insert(filename, index);
                    } else if filename.ends_with(".json") {
                        self.shared_files_idx.insert(filename, index);
                    }
                }
            }
            self.chunked_files_idx.push(chunk_map);
        }

        info!("... Splitted to {} chunks", self.chunked_files_idx.len());

        Ok(())
    }

    pub async fn export_chunks(&mut self) -> Result<()> {
        for (idx, chunk) in self.chunked_files_idx.clone().into_iter().enumerate() {
            info!(
                "Export {} of {} chunk, files: {}",
                idx + 1,
                self.chunked_files_idx.len(),
                chunk.len(),
            );
            let archive_name = self
                .config
                .slack_archive
                .file_name()
                .unwrap_or(OsStr::new("output.zip"))
                .to_str()
                .unwrap_or("output.zip")
                .to_string();

            let output = self
                .config
                .output
                .join(format!("chunk_{:03}_{}", idx, archive_name));

            info!("Output: {:?}", output);

            self.export_chunk(output, chunk).await?;
        }
        Ok(())
    }

    async fn export_chunk(&mut self, path: PathBuf, chunk: HashMap<String, usize>) -> Result<()> {
        // Create out file
        let mut out_file = File::create(path).await?;
        let mut writer = ZipFileWriter::with_tokio(&mut out_file);
        let mut downloads: Vec<Download> = Vec::new();

        self.pb = ProgressBar::new(chunk.len() as u64);
        self.pb.set_style(
              ProgressStyle::with_template(
                  "Export chunks: {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len}, ETA {eta})",
              )
              .unwrap(),
          );

        // Copy all shared files
        for (filename, idx) in self.shared_files_idx.clone().into_iter() {
            self.parse_and_copy_file(idx, filename, &mut writer, &mut downloads)
                .await?;
        }

        // Copy other files
        for (filename, idx) in chunk.into_iter() {
            self.pb.inc(1);
            self.parse_and_copy_file(idx, filename, &mut writer, &mut downloads)
                .await?;
        }

        info!("Start downloading {} files", downloads.len());

        let downloader = DownloaderBuilder::new()
            .concurrent_downloads(self.config.concurrent)
            .directory(self.config.output.clone())
            .build();
        let _summaries = downloader.download(&downloads).await;

        info!("Downloaded complete!..");

        // ADD Downloaded files to archive

        if !downloads.is_empty() {
            info!("Write upload files to zip archive...");
            self.zip_downloaded_files(&mut writer, &mut downloads)
                .await?;
        }

        writer.close().await?;

        info!("Done...");

        Ok(())
    }

    async fn zip_downloaded_files(
        &mut self,
        writer: &mut ZipFileWriter<Compat<&mut File>>,
        downloads: &mut Vec<Download>,
    ) -> Result<()> {
        let all_files = downloads.len();
        self.pb = ProgressBar::new(all_files as u64);
        self.pb.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len}, ETA {eta})",
                )
                .unwrap(),
            );

        for zip_path in downloads.iter() {
            let filename = zip_path.filename.clone();
            let full_name = self.config.output.join(filename.clone());

            let builder = ZipEntryBuilder::new(ZipString::from(filename), Compression::Deflate);
            // Read stream and write to writer...
            match writer
                .write_entry_stream(builder)
                .await
                .map(FuturesAsyncWriteCompatExt::compat_write)
            {
                Ok(mut writer) => {
                    // info!("Archive {} of {} files...", idx, all_files);
                    self.pb.inc(1);
                    match File::open(full_name.clone()).await {
                        Ok(mut file) => {
                            let mut reader = ReaderStream::with_capacity(&mut file, BUF_SIZE);
                            while let Some(chunk) = reader.next().await {
                                writer.write_all(&chunk?).await?;
                            }

                            writer.into_inner().close().await?;

                            if let Err(e) = tokio::fs::remove_file(full_name.clone()).await {
                                warn!("Failed to remove temp file {:?}: {}", full_name, e)
                            }
                        }
                        Err(e) => {
                            error!("Cannot open file... {:?}", full_name.to_str());
                            error!("Error {}", e);
                            continue;
                        }
                    }
                }
                Err(err) => error!(
                    "Error writing file {} to archive: {}",
                    zip_path.filename, err
                ),
            }
        }

        // Remove temp dir if esists
        let artefacts_dir = self.config.output.join("__uploads");

        tokio::fs::metadata(artefacts_dir.clone())
            .await
            .map(|metadata| (metadata, artefacts_dir.clone()))
            .map(|(metadata, dir)| async move {
                if metadata.is_dir() {
                    match tokio::fs::remove_dir_all(&dir).await {
                        Ok(_) => info!("Removed temp uploads directory: {:?}", self.config.output),
                        Err(e) => warn!(
                            "Failed to remove temp uploads directory {:?}: {}",
                            self.config.output, e
                        ),
                    }
                }
            })?
            .await;

        if tokio::fs::metadata(&artefacts_dir).await.is_ok() {
            info!("Removing temp uploads directory: {:?}", artefacts_dir);
        } else {
            warn!("Temp uploads directory not found: {:?}", artefacts_dir);
            return Ok(());
        }

        Ok(())
    }

    // Copy file from reader to writer
    async fn parse_and_copy_file(
        &mut self,
        idx: usize,
        filename: String,
        writer: &mut ZipFileWriter<Compat<&mut File>>,
        downloads: &mut Vec<Download>,
    ) -> Result<()> {
        let mut reader = self.reader.reader_with_entry(idx).await?;
        let mut buffer: Vec<u8> = Vec::new();
        reader.read_to_end(&mut buffer).await?;

        // Parse here...
        if let Ok(mut posts) = serde_json::from_slice::<Vec<model::SlackPost>>(&buffer) {
            for post in posts.iter_mut() {
                // For legacy posts...swap file to files..
                if let Some(file) = post.file.clone() {
                    post.files.push(file);
                    post.file = None;
                }

                if !post.files.is_empty() {
                    post.upload = true;

                    self.push_to_download(&post.files, downloads).await?;
                }
            }
            buffer = serde_json::to_vec(&posts)?;
        }

        let builder = ZipEntryBuilder::new(ZipString::from(filename), Compression::Deflate);
        writer.write_entry_whole(builder, &buffer).await?;
        Ok(())
    }

    async fn push_to_download(
        &mut self,
        files: &[model::File],
        downloads: &mut Vec<Download>,
    ) -> Result<()> {
        for file in files.iter() {
            let filename = format!("__uploads/{}/{}", file.id, file.name);
            // Download
            let url = reqwest::Url::parse(&file.url_for_download());
            match url {
                Ok(url) => {
                    downloads.push(Download { url, filename });
                }
                Err(e) => error!("Parse url {} error: {e}", &file.url_for_download()),
            }
        }
        Ok(())
    }
}
