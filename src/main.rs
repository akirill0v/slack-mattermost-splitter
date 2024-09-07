use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod downloader;
mod split;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,

    #[arg(short, long, default_value_t = 1)]
    concurrency: usize,

    #[arg(long, default_value_t = 100)]
    chunk_size: usize,

    #[arg(long, default_value_t = 0)]
    num_chunks: usize,

    #[arg(long, default_value_t = 5)]
    concurrent: usize,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Transform { input: String, output: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup default logger level:
    // - If RUST_LOG is set info
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    env_logger::init();

    let args = Args::parse();
    match args.cmd {
        Commands::Transform { input, output } => {
            let config = split::Config {
                slack_archive: PathBuf::from(input),
                output_archive: PathBuf::from(output),
                chunk_size: args.chunk_size,
                num_chunks: args.num_chunks,
                concurrent: args.concurrent,
            };
            let mut splitter = split::Splitter::new(config).await?;
            splitter.split().await?;
        }
    }
    Ok(())
}
