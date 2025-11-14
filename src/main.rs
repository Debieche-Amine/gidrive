use gidrive::drive;

use clap::{Parser, Subcommand};

// ──────────────────────────────────────────────────────────────
// CLI definition
// ──────────────────────────────────────────────────────────────
#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Upload a file: you must pass <REMOTE> and <LOCAL>
    Upload { remote: String, local: String },

    /// Download a file: you must pass <REMOTE> and <LOCAL>
    Download { remote: String, local: String },

    /// List files
    Ls,
}

// ──────────────────────────────────────────────────────────────
// Entry point
// ──────────────────────────────────────────────────────────────
fn main() {
    let cli = Cli::parse();
    match drive::init() {
        Ok(_) => println!("init done"),
        Err(e) => panic!("init returned err:{e}"),
    }

    match cli.command {
        Commands::Upload { remote, local } => match drive::upload_file(&remote, &local) {
            Ok(_) => println!("upload done"),
            Err(e) => panic!("upload returned err: {e}"),
        },

        Commands::Download { remote, local } => match drive::download_file(&remote, &local) {
            Ok(_) => println!("download done"),
            Err(e) => panic!("download returned err: {e}"),
        },

        Commands::Ls => match drive::list_metadata() {
            Ok(_) => println!("list done"),
            Err(e) => panic!("ls returned err: {e}"),
        },
    }
}
