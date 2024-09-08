use std::io::Cursor;
use std::path::{Path, PathBuf};

use futures_util::future::join_all;
use futures_util::StreamExt;
use log::info;
use reqwest::Response;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use tokio::io::AsyncWrite;
use tokio::{fs::File, io::BufReader};

use async_zip::{
    tokio::{read::seek::ZipFileReader, write::ZipFileWriter},
    Compression, ZipEntryBuilder, ZipString,
};

const BUF_SIZE: usize = 65536;

struct FileInfo {
    final_path: PathBuf,
}

impl FileInfo {
    fn new(url: &str, path: &PathBuf, response: &Response) -> Self {
        let file_name = url.split('/').next_back().unwrap();
        let file_name = file_name.split('?').next().unwrap();
        let final_path = path.join(file_name);

        let total_size_option = response.content_length();

        match total_size_option {
            Some(size) => size,
            None => panic!("no response length!"),
        };

        FileInfo { final_path }
    }
}

pub struct DownloaderClient<'a> {
    pub urls: &'a [String],
    pub path: &'a PathBuf,
}

impl<'a> DownloaderClient<'a> {
    pub fn new(urls: &'a [String], path: &'a PathBuf) -> Self {
        Self { urls, path }
    }

    pub async fn download_all(self) {
        let mut futures = Vec::new();
        for url in self.urls {
            futures.push(self.download_single(url, self.path));
        }

        let joined_futures = join_all(futures);
        joined_futures.await;
    }

    pub async fn download_single(
        &self,
        url: &str,
        path: &PathBuf,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = ClientBuilder::new(reqwest::Client::new())
            // Retry failed requests.
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        let response = client
            .get(url)
            // .header(ACCEPT, "application/pdf")
            .send()
            .await?;

        let file_info = FileInfo::new(url, path, &response);

        let mut stream = response.bytes_stream();

        let mut file = File::create(format!("{}", file_info.final_path.display())).await?;

        while let Some(chunk) = stream.next().await {
            let chunk_data = chunk.unwrap();

            let mut content = Cursor::new(chunk_data);
            tokio::io::copy(&mut content, &mut file).await?;
        }

        Ok(())
    }
}
