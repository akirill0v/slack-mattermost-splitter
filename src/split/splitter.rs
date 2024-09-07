use std::{collections::HashMap, path::PathBuf};

use anyhow::Result;
use async_zip::{
    base::{read::seek::ZipFileReader, write::ZipFileWriter},
    Compression, ZipEntryBuilder, ZipString,
};
use futures::AsyncReadExt;
use log::info;
use tokio::{fs::File, io::BufReader};
use tokio_util::compat::Compat;

use crate::downloader::chunked::{Download, DownloaderClient};

use super::{model, Config};

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
}

impl Splitter {
    pub async fn new(config: Config) -> Result<Self> {
        info!("Open zip file...");
        let reader = ZipFileReader::with_tokio(BufReader::new(
            File::open(config.slack_archive.clone()).await?,
        ))
        .await?;
        Ok(Splitter {
            reader,
            config,
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

        let chunk_size = if self.config.num_chunks > 0 {
            total_entries / self.config.num_chunks
        } else {
            self.config.chunk_size
        };

        info!("Split {total_entries} files to chunks by {chunk_size} items maximum...");

        for (chunk_idx, chunk) in self.reader.file().entries().chunks(chunk_size).enumerate() {
            let mut chunk_map: HashMap<String, usize> = HashMap::new();
            for (entry_idx, entry) in chunk.iter().enumerate() {
                let index = chunk_size * chunk_idx + entry_idx;
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

            let output = self.config.output_archive.clone();
            let output_dir = output
                .parent()
                .unwrap_or_else(|| std::path::Path::new("/tmp"));
            let output_filename = output.file_name().unwrap().to_str().unwrap();
            let output = output_dir.join(format!("chunk_{:03}_{}", idx, output_filename));

            info!("Output: {:?}", output);
            self.export_chunk(output, idx, chunk).await?;
        }
        Ok(())
    }

    async fn export_chunk(
        &mut self,
        path: PathBuf,
        c_idx: usize,
        chunk: HashMap<String, usize>,
    ) -> Result<()> {
        // Create out file
        let mut out_file = File::create(path).await?;
        let mut writer = ZipFileWriter::with_tokio(&mut out_file);
        let mut downloads: Vec<Download> = Vec::new();

        // Copy all shared files
        for (filename, idx) in self.shared_files_idx.clone().into_iter() {
            self.parse_and_copy_file(idx, filename, &mut writer, &mut downloads)
                .await?;
        }

        // Copy other files
        for (filename, idx) in chunk.into_iter() {
            self.parse_and_copy_file(idx, filename, &mut writer, &mut downloads)
                .await?;
        }

        let all_files_for_download = downloads.len();

        info!("Start downloading {} files", downloads.len());
        for (chunk_idx, chunk_downloads) in downloads.chunks(self.config.concurrent).enumerate() {
            let mut client = DownloaderClient::new(chunk_downloads);
            let downloaded_files = client.download_many().await?;

            for (idx, d) in downloaded_files.iter().enumerate() {
                let builder =
                    ZipEntryBuilder::new(ZipString::from(d.out.clone()), Compression::Deflate);
                info!(
                    "Write (Chunk:{}, {} of {}) file {}",
                    c_idx + 1,
                    chunk_idx * self.config.concurrent + idx,
                    all_files_for_download,
                    d.out
                );
                writer.write_entry_whole(builder, d.data.as_slice()).await?;
            }
        }

        info!("Downloaded complete!..");

        writer.close().await?;
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
            let download = Download {
                url: file.url_for_download(),
                out: filename,
                data: Vec::new(),
            };
            downloads.push(download);
        }
        Ok(())
    }
}
