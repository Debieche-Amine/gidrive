use clap::{Parser, Subcommand};
use gidrive::api;

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
    /// Clean temporary or cached files
    Clean,
}

// ──────────────────────────────────────────────────────────────
// Entry point
// ──────────────────────────────────────────────────────────────
fn main() {
    let cli = Cli::parse();

    match api::init() {
        Ok(_) => println!("--- init done"),
        Err(e) => panic!("--- init returned err:{e}"),
    }

    match cli.command {
        Commands::Upload { remote, local } => match api::upload(&remote, &local) {
            Ok(_) => println!("--- upload done"),
            Err(e) => panic!("--- upload returned err: {e}"),
        },
        Commands::Download { remote, local } => match api::download(&remote, &local) {
            Ok(_) => println!("--- download done"),
            Err(e) => panic!("--- download returned err: {e}"),
        },
        Commands::Ls => match api::ls() {
            Ok(_) => println!("--- list done"),
            Err(e) => panic!("--- ls returned err: {e}"),
        },
        Commands::Clean => match api::clean() {
            Ok(_) => println!("--- clean done"),
            Err(e) => panic!("--- clean returned err: {e}"),
        },
    }
}
