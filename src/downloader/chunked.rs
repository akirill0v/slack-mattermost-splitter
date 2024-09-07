use anyhow::Result;
use futures::future::join_all;
use log::error;
use reqwest::Client;

#[derive(Debug, Clone)]
pub struct Download {
    pub url: String,
    pub out: String,
    pub data: Vec<u8>,
}

pub struct DownloaderClient<'a> {
    pub downloads: &'a [Download],
}

impl<'a> DownloaderClient<'a> {
    pub fn new(downloads: &'a [Download]) -> Self {
        Self { downloads }
    }

    pub async fn download_many(&mut self) -> Result<Vec<Download>> {
        join_all(self.downloads.iter().map(|d| self.download_single(d)))
            .await
            .into_iter()
            .collect()
    }

    pub async fn download_single(&self, download: &'a Download) -> Result<Download> {
        if download.url.is_empty() {
            // Just empty file
            error!("Empty url for downloading: {}", download.out);
            return Ok(download.clone());
        }

        let client = Client::new();
        let mut ret_download = download.clone();
        match client.get(download.url.clone()).send().await {
            Ok(response) => {
                let bytes = response.bytes().await?;
                ret_download.data = bytes.to_vec();
            }
            Err(e) => {
                error!("Error: {}, {}", download.out, download.url);
                error!("Downloader error: {e}\nSkip for now, continue downloading next file...")
            }
        }

        Ok(ret_download)
    }
}
