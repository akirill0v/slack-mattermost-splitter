use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod split;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,

    #[arg(long, default_value_t = 100)]
    chunk_size: usize,

    #[arg(long, default_value_t = 1)]
    num_chunks: usize,

    #[arg(long, default_value_t = 5)]
    concurrent: usize,

    #[arg(long, default_value_t = false)]
    skip_downloading: bool,

    #[arg(long, default_value_t = false)]
    skip_directs: bool,

    #[arg(long, default_value_t = false)]
    skip_channels: bool,
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
                output: PathBuf::from(output),
                chunk_size: args.chunk_size,
                num_chunks: args.num_chunks,
                concurrent: args.concurrent,
                skip_directs: args.skip_directs,
                skip_channels: args.skip_channels,
                skip_downloading: args.skip_downloading,
            };
            let mut splitter = split::Splitter::new(config).await?;
            splitter.split().await?;
        }
    }
    Ok(())
}
