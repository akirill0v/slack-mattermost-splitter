use std::{collections::HashMap, ffi::OsStr, path::PathBuf};

use anyhow::{anyhow, bail, Error, Result};
use async_zip::{
    base::{read::seek::ZipFileReader, write::ZipFileWriter},
    Compression, ZipEntryBuilder, ZipString,
};
use futures::{AsyncReadExt, SinkExt, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info, warn};
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufReader},
};
use tokio_util::{
    compat::{Compat, FuturesAsyncWriteCompatExt},
    io::ReaderStream,
};
use trauma::{download::Download, downloader::DownloaderBuilder};

use super::{
    model::{self, Chunk, ChunkItem, Direct},
    Config,
};

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
    chunked_files_idx: Vec<Chunk>,
    // files_idx: HashMap<String, usize>,
    // HashMap with channel keys, contains (filename, index_in_arch)
    grouped_files_idx: HashMap<String, Vec<(String, usize)>>,

    directs: Vec<Direct>,
    // HashMap with direct_channel keys, contains (filename, index_in_arch)
    direct_files_idx: HashMap<String, Vec<(String, usize)>>,
    chunked_directs_idx: Vec<Chunk>,

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
            // files_idx: HashMap::new(),
            directs: Vec::new(),
            grouped_files_idx: HashMap::new(),
            direct_files_idx: HashMap::new(),
            chunked_directs_idx: Vec::new(),
        })
    }

    pub async fn split(&mut self) -> Result<()> {
        info!("Sptit..");
        self.scan_files().await?;
        if !self.config.skip_directs {
            self.fetch_directs().await?;
            self.split_directs_to_chunks();
            self.export_directs_chunks().await?;
        }
        if !self.config.skip_channels {
            self.split_files_to_chunks();
            self.export_channels_chunks().await?;
        }
        Ok(())
    }

    pub async fn fetch_directs(&mut self) -> Result<()> {
        let dms_file_idx = match self.shared_files_idx.get("dms.json") {
            Some(idx) => *idx,
            None => {
                bail!("dms.json not found in shared files");
            }
        };

        let mut reader = self.reader.reader_with_entry(dms_file_idx).await?;
        let mut buffer: Vec<u8> = Vec::new();
        reader.read_to_end(&mut buffer).await?;

        self.directs = match serde_json::from_slice(&buffer) {
            Ok(dms) => dms,
            Err(e) => {
                bail!("Failed to deserialize dms.json: {}", e);
            }
        };

        info!(
            "Successfully deserialized dms.json with {} entries",
            self.directs.len()
        );

        for dm in self.directs.iter() {
            if let Some((dir, files)) = self.grouped_files_idx.get_key_value(&dm.id) {
                self.direct_files_idx.insert(dir.clone(), files.clone());
            } else {
                error!("Cannot find direct {} in grouped files", &dm.id);
            }

            self.grouped_files_idx.remove(&dm.id);
        }

        info!(
            "Successfully copied directs: {}...",
            self.direct_files_idx.len()
        );

        Ok(())
    }

    pub async fn scan_files(&mut self) -> Result<()> {
        info!("Scan zip file structure and split to chunks...");
        dbg!(&self.config);

        let all_entries = self.reader.file().entries();

        self.pb = ProgressBar::new(all_entries.len() as u64);
        self.pb.set_style(
                ProgressStyle::with_template(
                    "Scan files: {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len}, ETA {eta})",
                )
                .unwrap(),
            );

        for (idx, entry) in all_entries.iter().enumerate() {
            self.pb.inc(1);
            if let Ok(filename) = entry.filename().clone().into_string() {
                if filename.ends_with(".json") && !SHARED_NAMES.contains(&filename.as_ref()) {
                    let dirname = filename.split('/').next().unwrap_or("-");

                    let entry = self
                        .grouped_files_idx
                        .entry(dirname.to_string())
                        .or_default();

                    entry.push((filename, idx));
                    // self.files_idx.insert(filename, idx);
                } else if filename.ends_with(".json") {
                    self.shared_files_idx.insert(filename, idx);
                }
            }
        }
        self.pb.finish();
        info!("Scanned: {} files", all_entries.len());
        info!(
            "Fetch {} groups and direct channels",
            self.grouped_files_idx.len()
        );
        Ok(())
    }

    // Split direct files to chunks by full channels
    fn split_directs_to_chunks(&mut self) {
        let keys: Vec<String> = self
            .direct_files_idx
            .keys()
            .map(|k| k.to_string())
            .collect();

        let chunk_size = (keys.len() as f64 / self.config.num_chunks as f64).ceil() as usize;

        for chunked_keys in keys.chunks(chunk_size) {
            let mut chunk = Chunk::default();
            for key in chunked_keys {
                let mut chunk_item = ChunkItem {
                    id: key.clone(),
                    files: Vec::new(),
                };
                if let Some(files) = self.direct_files_idx.get(key) {
                    for (filename, idx) in files {
                        chunk_item.files.push((filename.to_string(), *idx));
                    }
                }
                chunk.items.push(chunk_item);
            }
            self.chunked_directs_idx.push(chunk);
        }
    }

    // Split all files to chunks by full channels
    fn split_files_to_chunks(&mut self) {
        let keys: Vec<String> = self
            .grouped_files_idx
            .keys()
            .map(|k| k.to_string())
            .collect();

        let chunk_size = (keys.len() as f64 / self.config.num_chunks as f64).ceil() as usize;

        for chunked_keys in keys.chunks(chunk_size) {
            let mut chunk = Chunk::default();
            for key in chunked_keys {
                let mut chunk_item = ChunkItem {
                    id: key.clone(),
                    files: Vec::new(),
                };
                if let Some(files) = self.grouped_files_idx.get(key) {
                    for (filename, idx) in files {
                        chunk_item.files.push((filename.to_string(), *idx));
                    }
                }
                chunk.items.push(chunk_item);
            }
            self.chunked_files_idx.push(chunk);
        }
    }

    pub async fn export_directs_chunks(&mut self) -> Result<()> {
        let archive_name = self
            .config
            .slack_archive
            .file_name()
            .unwrap_or(OsStr::new("directs.zip"))
            .to_str()
            .unwrap_or("directs.zip")
            .to_string();

        for (idx, chunk) in self.chunked_directs_idx.clone().into_iter().enumerate() {
            info!(
                "Export {} of {} directs chunk: {}",
                idx + 1,
                self.chunked_directs_idx.len(),
                chunk.items.len(),
            );
            let output = self
                .config
                .output
                .join(format!("directs_{:03}_{}", idx, archive_name));

            info!("Output: {:?}", output);

            let shared_files = vec!["users.json"];
            // Filter chunked directs and export them to dms.json
            // let keys: Vec<String> = chunk.item.keys().clone().map(|c| c.to_string()).collect();
            let keys: Vec<String> = chunk.items.iter().map(|ci| ci.id.clone()).collect();
            let chunked_directs: Vec<&Direct> = self
                .directs
                .iter()
                .filter(|d| keys.contains(&d.id))
                .collect();
            info!("Filtered data: {} directs in chunk", chunked_directs.len());
            let data = serde_json::to_vec(&chunked_directs)?;
            let mut additional_data = HashMap::new();
            additional_data.insert(String::from("dms.json"), data.as_slice());

            self.export_chunk(output, chunk, shared_files, &additional_data)
                .await?;
        }

        Ok(())
    }

    pub async fn export_channels_chunks(&mut self) -> Result<()> {
        let archive_name = self
            .config
            .slack_archive
            .file_name()
            .unwrap_or(OsStr::new("channels.zip"))
            .to_str()
            .unwrap_or("channels.zip")
            .to_string();

        for (idx, chunk) in self.chunked_files_idx.clone().into_iter().enumerate() {
            info!(
                "Export {} of {} channels chunks: {}",
                idx + 1,
                self.chunked_files_idx.len(),
                chunk.items.len(),
            );
            let output = self
                .config
                .output
                .join(format!("channels_{:03}_{}", idx, archive_name));

            info!("Output: {:?}", output);

            let shared_files = vec!["users.json", "channels.json", "groups.json", "mpims.json"];

            self.export_chunk(output, chunk, shared_files, &HashMap::new())
                .await?;
        }

        Ok(())
    }

    async fn export_chunk(
        &mut self,
        path: PathBuf,
        chunk: Chunk,
        shared_files: Vec<&str>,
        additional_data: &HashMap<String, &[u8]>,
    ) -> Result<()> {
        // Create out file
        let mut out_file = File::create(path).await?;
        let mut writer = ZipFileWriter::with_tokio(&mut out_file);
        let mut downloads: Vec<Download> = Vec::new();

        self.pb = ProgressBar::new(chunk.items.len() as u64);
        self.pb.set_style(
              ProgressStyle::with_template(
                  "Export chunks: {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len}, ETA {eta})",
              )
              .unwrap(),
          );

        // Copy all shared files
        for (filename, idx) in self
            .shared_files_idx
            .clone()
            .into_iter()
            .filter(|f| shared_files.contains(&f.0.as_str()))
        {
            self.parse_and_copy_file(idx, filename, &mut writer, &mut downloads)
                .await?;
        }

        // Copy other files
        for ci in chunk.items.into_iter() {
            self.pb.inc(1);
            for (filename, idx) in ci.files {
                self.parse_and_copy_file(idx, filename, &mut writer, &mut downloads)
                    .await?;
            }
        }

        for (filename, data) in additional_data {
            self.write_file(&mut writer, filename.clone(), data).await?;
        }

        self.pb.finish();

        if self.config.skip_downloading {
            warn!("Skip downloading artefacts!!!");
        } else {
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
                self.zip_downloaded_files(&mut writer, self.config.output.join("__uploads"))
                    .await?;
            }
        }

        writer.close().await?;

        info!("Done...");

        Ok(())
    }

    async fn zip_downloaded_files(
        &mut self,
        writer: &mut ZipFileWriter<Compat<&mut File>>,
        artefacts_dir: PathBuf,
    ) -> Result<()> {
        info!("Read files from {:?}", artefacts_dir);

        let mut files_to_zip = Vec::new();
        let mut stack = vec![artefacts_dir.clone()];

        // Recursively traverse the directory structure
        while let Some(dir) = stack.pop() {
            let mut read_dir = tokio::fs::read_dir(dir).await?;
            while let Some(entry) = read_dir.next_entry().await? {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.is_file() {
                    let filename = path
                        .strip_prefix(&self.config.output)?
                        .to_string_lossy()
                        .into_owned();
                    files_to_zip.push((path, filename));
                }
            }
        }

        self.pb = ProgressBar::new(files_to_zip.len() as u64);
        self.pb.set_style(
                ProgressStyle::with_template(
                    "Add to zip: {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len}, ETA {eta})",
                )
                .unwrap(),
            );

        // Add files to archive
        for (path, filename) in files_to_zip.iter() {
            let builder =
                ZipEntryBuilder::new(ZipString::from(filename.as_str()), Compression::Deflate);
            match writer
                .write_entry_stream(builder)
                .await
                .map(FuturesAsyncWriteCompatExt::compat_write)
            {
                Ok(mut zip_writer) => {
                    self.pb.inc(1);
                    match File::open(path.clone()).await {
                        Ok(mut file) => {
                            let mut reader = ReaderStream::with_capacity(&mut file, BUF_SIZE);
                            while let Some(chunk) = reader.next().await {
                                zip_writer.write_all(&chunk?).await?;
                            }
                            zip_writer.into_inner().close().await?;
                            if let Err(e) = tokio::fs::remove_file(path.clone()).await {
                                warn!("Failed to remove temp file {:?}: {}", path, e);
                            }
                        }
                        Err(e) => {
                            error!("Cannot open file... {:?}", path.to_str());
                            error!("Error {}", e);
                            continue;
                        }
                    }
                }
                Err(err) => error!("Error writing file {} to archive: {}", filename, err),
            }
        }

        // Cleanup __uploads directory
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

        Ok(())
    }

    async fn write_file(
        &mut self,
        writer: &mut ZipFileWriter<Compat<&mut File>>,
        filename: String,
        data: &[u8],
    ) -> Result<()> {
        let builder = ZipEntryBuilder::new(ZipString::from(filename), Compression::Deflate);
        writer
            .write_entry_whole(builder, data)
            .await
            .map_err(Error::from)
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

                if !post.files.is_empty()
                    && post.files.iter().any(|f| !f.url_for_download().is_empty())
                {
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
        for file in files
            .iter()
            .filter(|f| !f.is_external && !f.url_for_download().is_empty())
        {
            let filename = format!("__uploads/{}/{}", file.id, file.name);
            // Download
            let url = reqwest::Url::parse(&file.url_for_download());
            match url {
                Ok(url) => {
                    downloads.push(Download { url, filename });
                }
                Err(e) => {
                    error!("Parse url {} error: {e}", &file.url_for_download())
                }
            }
        }
        Ok(())
    }
}
