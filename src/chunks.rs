use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

use crate::constants::{GITHUB_USERNAME, TMPFS_DIR};
use crate::git::{clone_repo, git_add_commit_push};

pub fn upload_chunks_to_repo(
    checksum: &str,
    repo_name: &str,
    chunk_list: &[(usize, PathBuf, String)],
) -> Result<()> {
    let repo_url = format!("git@github.com:{}/{}.git", GITHUB_USERNAME, repo_name);
    let clone_dir = PathBuf::from(TMPFS_DIR).join(repo_name);
    if clone_dir.exists() {
        std::fs::remove_dir_all(&clone_dir).context("Failed to remove existing clone")?;
    }
    clone_repo(&repo_url, &clone_dir)?;
    for (_index, chunk_path, dest_path) in chunk_list {
        let dest = clone_dir.join(dest_path);
        std::fs::copy(chunk_path, &dest).context("Failed to copy chunk to repo")?;
    }
    git_add_commit_push(
        &clone_dir,
        &format!("Add {} chunks for {}", chunk_list.len(), checksum),
    )?;
    std::fs::remove_dir_all(&clone_dir).context("Failed to clean up data repo clone")?;
    Ok(())
}

pub fn download_chunks_from_repo(
    repo_name: &str,
    chunk_list: &[(usize, String)],
    temp_dir: &Path,
) -> Result<()> {
    let repo_url = format!("git@github.com:{}/{}.git", GITHUB_USERNAME, repo_name);
    let clone_dir = PathBuf::from(TMPFS_DIR).join(format!("dl_{}", repo_name));
    if clone_dir.exists() {
        std::fs::remove_dir_all(&clone_dir).context("Failed to remove existing dl clone")?;
    }
    clone_repo(&repo_url, &clone_dir)?;
    for (global_i, chunk_path_str) in chunk_list {
        let src = clone_dir.join(chunk_path_str);
        let dst = temp_dir.join(format!("chunk_{}", global_i));
        std::fs::copy(&src, &dst).context("Failed to copy chunk from repo")?;
    }
    std::fs::remove_dir_all(&clone_dir).context("Failed to clean up dl repo clone")?;
    Ok(())
}
