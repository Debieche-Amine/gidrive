use anyhow::{Context, Result};
use serde_json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::constants::{MAX_SIZE_PER_REPO, TMPFS_DIR, VERSION};
use crate::git::{create_repo, repo_exists};
use crate::models::{RepoInfo, ReposMetadata};

pub fn get_metadata_dir() -> PathBuf {
    PathBuf::from(TMPFS_DIR).join("metadata")
}

pub fn load_repos_metadata(metadata_clone_dir: &Path) -> Result<ReposMetadata> {
    let path = metadata_clone_dir.join("repos.json");
    if path.exists() {
        let data = std::fs::read_to_string(&path).context(
            "Failed to read repos.json, uncompatible versions? repos.json modified manually?",
        )?;
        serde_json::from_str(&data).context("Failed to parse repos.json")
    } else {
        Ok(ReposMetadata {
            next_id: 1,
            repos: BTreeMap::new(),
        })
    }
}

pub fn save_repos_metadata(metadata_clone_dir: &Path, repos_meta: &ReposMetadata) -> Result<()> {
    let path = metadata_clone_dir.join("repos.json");
    let data =
        serde_json::to_string_pretty(repos_meta).context("Failed to serialize repos.json")?;
    std::fs::write(&path, data).context("Failed to write repos.json")
}

pub fn load_version(metadata_clone_dir: &Path) -> Result<String> {
    let path = metadata_clone_dir.join("version.txt");
    if path.exists() {
        let data = std::fs::read_to_string(&path).context("Failed to read version.txt")?;
        Ok(data.trim().to_string())
    } else {
        save_version(metadata_clone_dir, VERSION)?;
        Ok(VERSION.to_string())
    }
}

pub fn save_version(metadata_clone_dir: &Path, version: &str) -> Result<()> {
    let path = metadata_clone_dir.join("version.txt");
    std::fs::write(&path, version).context("Failed to write version.txt")
}

pub fn find_or_create_repo_for_chunk(
    repos_meta: &mut ReposMetadata,
    chunk_size: u64,
) -> Result<String> {
    for (_, repo) in repos_meta.repos.iter_mut() {
        if repo.current_size + chunk_size <= MAX_SIZE_PER_REPO {
            repo.current_size += chunk_size;
            return Ok(repo.name.clone());
        }
    }
    // Create new repo
    let repo_id = repos_meta.next_id;
    repos_meta.next_id += 1;
    let repo_name = format!("storage-{:04}", repo_id);
    if !repo_exists(&repo_name) {
        create_repo(&repo_name).context("Failed to create new repo")?;
    }
    repos_meta.repos.insert(
        repo_name.clone(),
        RepoInfo {
            name: repo_name.clone(),
            current_size: chunk_size,
        },
    );
    Ok(repo_name)
}
