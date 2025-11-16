use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;
use std::process::Command;

use crate::constants::{CHUNK_SIZE, TMPFS_DIR};

pub fn run(cmd: &str) -> io::Result<String> {
    let output = Command::new("sh").arg("-c").arg(cmd).output()?;
    io::stdout().write_all(&output.stdout)?;
    io::stderr().write_all(&output.stderr)?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Command failed: {}", cmd),
        ))
    }
}

pub fn get_file_sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path).context("Failed to open file for hashing")?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; CHUNK_SIZE];
    loop {
        let bytes_read = file.read(&mut buffer).context("Failed to read for hash")?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

pub fn ensure_tmpfs_dir() -> Result<()> {
    std::fs::create_dir_all(TMPFS_DIR).context("Failed to create TMPFS_DIR")
}

pub fn human_size(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    const TB: f64 = GB * 1024.0;
    let b = bytes as f64;
    if b < KB {
        format!("{} B", bytes)
    } else if b < MB {
        format!("{:.2} KB", b / KB)
    } else if b < GB {
        format!("{:.2} MB", b / MB)
    } else if b < TB {
        format!("{:.2} GB", b / GB)
    } else {
        format!("{:.2} TB", b / TB)
    }
}

pub fn versions_are_compatible(found: &str, current: &str) -> bool {
    let found_parts: Vec<&str> = found.split('.').collect();
    let current_parts: Vec<&str> = current.split('.').collect();

    if found_parts.len() != 3 || current_parts.len() != 3 {
        return false;
    }

    let Ok(found_maj) = found_parts[0].parse::<u32>() else {
        return false;
    };
    let Ok(found_min) = found_parts[1].parse::<u32>() else {
        return false;
    };
    let Ok(curr_maj) = current_parts[0].parse::<u32>() else {
        return false;
    };
    let Ok(curr_min) = current_parts[1].parse::<u32>() else {
        return false;
    };

    if found_maj != curr_maj {
        return false;
    }
    if curr_maj == 0 && found_min != curr_min {
        return false;
    }
    true
}
