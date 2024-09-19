use std::path::PathBuf;

#[derive(Debug)]
pub struct Config {
    pub slack_archive: PathBuf,
    pub output: PathBuf,
    pub chunk_size: usize,
    pub num_chunks: usize,
    pub concurrent: usize,

    pub skip_downloading: bool,
    pub skip_directs: bool,
    pub skip_channels: bool,
}
