use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone)]
pub struct ChunkInfo {
    pub repo: String,
    pub path: String,
    pub size: u64,
    pub index: usize,
}

#[derive(Serialize, Deserialize)]
pub struct FileMetadata {
    pub checksum: String,
    pub size: u64,
    pub chunks: Vec<ChunkInfo>,
}

#[derive(Serialize, Deserialize)]
pub struct RepoInfo {
    pub name: String,
    pub current_size: u64,
}

#[derive(Serialize, Deserialize)]
pub struct ReposMetadata {
    pub next_id: usize,
    pub repos: BTreeMap<String, RepoInfo>,
}
