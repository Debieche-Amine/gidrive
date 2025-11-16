use anyhow::{anyhow, Context, Result};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde_json;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::chunks::{download_chunks_from_repo, upload_chunks_to_repo};
use crate::constants::{
    CHUNK_SIZE, METADATA_REPO_URL, NUM_PUSH_THREADS, SSH_KEY_PATH, TMPFS_DIR, VERSION,
};
use crate::git::{
    clone_repo, create_repo, delete_repo, git_add_commit_push, list_repos, repo_exists, ssh_agent,
};
use crate::metadata::{
    find_or_create_repo_for_chunk, get_metadata_dir, load_repos_metadata, load_version,
    save_repos_metadata,
};
use crate::models::{ChunkInfo, FileMetadata, ReposMetadata};
use crate::utils::{
    ensure_tmpfs_dir, get_file_sha256, human_size, retry, sleep, versions_are_compatible,
};

pub fn upload(remote: &str, local: &str) -> Result<()> {
    ensure_tmpfs_dir()?;
    let local_path = Path::new(local);
    let checksum = get_file_sha256(local_path)?;
    let file_size = fs::metadata(local_path)?.len();
    // Clone metadata to get repos info
    let metadata_clone_dir = get_metadata_dir();
    if metadata_clone_dir.exists() {
        fs::remove_dir_all(&metadata_clone_dir)?;
    }
    clone_repo(METADATA_REPO_URL, &metadata_clone_dir)?;

    // check if current version and remote version are compatable
    let version = load_version(&metadata_clone_dir)?;
    if !versions_are_compatible(&version, VERSION) {
        panic!(
            "upload rejected: Incompatible version: current {}, found {}, you can only perform read operations",
            VERSION, version
        );
    }
    let mut repos_meta = load_repos_metadata(&metadata_clone_dir)?;

    // Pre-assign repos for all chunks (sequential)
    let mut assignments: Vec<(usize, String, u64)> = Vec::new();
    let mut remaining = file_size;
    let mut index = 0;
    while remaining > 0 {
        let chunk_size = remaining.min(CHUNK_SIZE as u64);
        let repo_name = retry(
            || find_or_create_repo_for_chunk(&mut repos_meta, chunk_size),
            3,  // start delay 1 second
            10, // add 1 second each retry; use 0 if you want fixed delay
        );
        sleep(1.3);
        assignments.push((index, repo_name, chunk_size));
        remaining -= chunk_size;
        index += 1;
    }
    // Save and push updated repos.json
    save_repos_metadata(&metadata_clone_dir, &repos_meta)?;
    git_add_commit_push(&metadata_clone_dir, "Pre-assign repos for upload")?;
    // Create temp chunk files sequentially
    let mut chunk_paths: Vec<PathBuf> = Vec::new();
    let mut file = BufReader::new(File::open(local_path)?);
    for (_index, _repo, chunk_size) in &assignments {
        let chunk_tmp_path =
            PathBuf::from(TMPFS_DIR).join(format!("chunk_u_{}", chunk_paths.len()));
        let mut chunk_file = BufWriter::new(File::create(&chunk_tmp_path)?);
        let mut to_read = *chunk_size as usize;
        while to_read > 0 {
            let buf_size = to_read.min(8192);
            let mut buf = vec![0u8; buf_size];
            let read = file.read(&mut buf)?;
            chunk_file.write_all(&buf[..read])?;
            to_read -= read;
            if read == 0 {
                break;
            }
        }
        chunk_file.flush()?;
        chunk_paths.push(chunk_tmp_path);
    }
    // Group chunks by repo for batched parallel upload
    let mut repo_map: HashMap<String, Vec<(usize, PathBuf, String)>> = HashMap::new();
    for (idx, (i, repo, _)) in assignments.iter().enumerate() {
        let dest_path = format!("{}_{:04}.chunk", checksum, *i);
        repo_map.entry(repo.clone()).or_insert_with(Vec::new).push((
            *i,
            chunk_paths[idx].clone(),
            dest_path,
        ));
    }
    // Parallel upload per repo (batched)
    let _results: Vec<Result<(), anyhow::Error>> = repo_map
        .par_iter()
        .map(|(repo_name, chunk_list)| upload_chunks_to_repo(&checksum, repo_name, chunk_list))
        .collect();
    // Cleanup temp chunks
    for chunk_path in chunk_paths {
        let _ = fs::remove_file(chunk_path);
    }
    // Re-clone metadata for fresh state and write file metadata
    if metadata_clone_dir.exists() {
        fs::remove_dir_all(&metadata_clone_dir)?;
    }
    clone_repo(METADATA_REPO_URL, &metadata_clone_dir)?;
    let fs_dir = metadata_clone_dir.join("fs");
    let remote_path = Path::new(remote);
    let file_name = remote_path
        .file_name()
        .context("Remote path must have a file name")?;
    let meta_file_name = format!("{}.json", file_name.to_string_lossy());
    let parent = remote_path.parent().unwrap_or(Path::new(""));
    let file_meta_path = fs_dir.join(parent).join(meta_file_name);
    fs::create_dir_all(
        file_meta_path
            .parent()
            .context("Failed to get parent for file meta")?,
    )?;
    let chunks: Vec<ChunkInfo> = assignments
        .iter()
        .map(|(i, r, s)| ChunkInfo {
            repo: r.clone(),
            path: format!("{}_{:04}.chunk", checksum, *i),
            size: *s,
            index: *i,
        })
        .collect();
    let file_meta = FileMetadata {
        checksum: checksum.clone(),
        size: file_size,
        chunks,
    };
    let data = serde_json::to_string_pretty(&file_meta).context("Failed to serialize file meta")?;
    fs::write(&file_meta_path, data).context("Failed to write file meta")?;
    git_add_commit_push(&metadata_clone_dir, &format!("Add metadata for {}", remote))?;
    fs::remove_dir_all(&metadata_clone_dir)?;
    Ok(())
}

pub fn download(remote: &str, local: &str) -> Result<()> {
    ensure_tmpfs_dir()?;
    let metadata_clone_dir = get_metadata_dir();
    if metadata_clone_dir.exists() {
        fs::remove_dir_all(&metadata_clone_dir)?;
    }
    clone_repo(METADATA_REPO_URL, &metadata_clone_dir)?;
    let fs_dir = metadata_clone_dir.join("fs");
    let remote_path = Path::new(remote);
    let file_name = remote_path
        .file_name()
        .context("Remote path must have a file name")?;
    let meta_file_name = format!("{}.json", file_name.to_string_lossy());
    let parent = remote_path.parent().unwrap_or(Path::new(""));
    let file_meta_path = fs_dir.join(parent).join(meta_file_name);
    if !file_meta_path.exists() {
        return Err(anyhow::anyhow!("File metadata not found for {}", remote));
    }
    let data = fs::read_to_string(&file_meta_path)?;
    let mut file_meta: FileMetadata = serde_json::from_str(&data)?;
    // Sort chunks by index
    file_meta.chunks.sort_by_key(|c| c.index);
    // Group chunks by repo for batched parallel download
    let mut repo_map: HashMap<String, Vec<(usize, String)>> = HashMap::new();
    for (global_i, chunk) in file_meta.chunks.iter().enumerate() {
        repo_map
            .entry(chunk.repo.clone())
            .or_insert_with(Vec::new)
            .push((global_i, chunk.path.clone()));
    }
    let temp_dir = PathBuf::from(TMPFS_DIR).join(format!("dl_{}", file_meta.checksum));
    fs::create_dir_all(&temp_dir).context("Failed to create dl temp dir")?;
    // Parallel download per repo (batched)
    let _results: Vec<Result<(), anyhow::Error>> = repo_map
        .par_iter()
        .map(|(repo_name, chunk_list)| download_chunks_from_repo(repo_name, chunk_list, &temp_dir))
        .collect();
    // Concatenate chunks in order to local file
    let local_path = Path::new(local);
    fs::create_dir_all(
        local_path
            .parent()
            .context("Failed to create local parent dir")?,
    )?;
    let mut output = BufWriter::new(File::create(local_path)?);
    let mut total_written = 0u64;
    for i in 0..file_meta.chunks.len() {
        let chunk_p = temp_dir.join(format!("chunk_{}", i));
        let mut chunk_r =
            BufReader::new(File::open(&chunk_p).context("Failed to open downloaded chunk")?);
        total_written +=
            io::copy(&mut chunk_r, &mut output).context("Failed to copy chunk to output")?;
        fs::remove_file(&chunk_p).context("Failed to remove temp chunk")?;
    }
    output.flush().context("Failed to flush output")?;
    fs::remove_dir(&temp_dir).context("Failed to remove dl temp dir")?;
    if total_written != file_meta.size {
        return Err(anyhow::anyhow!(
            "Downloaded size mismatch: {} vs {}",
            total_written,
            file_meta.size
        ));
    }
    let downloaded_checksum = get_file_sha256(local_path)?;
    if downloaded_checksum != file_meta.checksum {
        let _ = fs::remove_file(local_path);
        return Err(anyhow::anyhow!(
            "Checksum mismatch: {} vs {}",
            downloaded_checksum,
            file_meta.checksum
        ));
    }
    fs::remove_dir_all(&metadata_clone_dir)?;
    Ok(())
}

pub fn init() -> Result<()> {
    ssh_agent(SSH_KEY_PATH);
    if !repo_exists("metadata") {
        create_repo("metadata")?;
    }
    ensure_tmpfs_dir()?;
    ThreadPoolBuilder::new()
        .num_threads(NUM_PUSH_THREADS)
        .build_global()
        .context("Failed to initialize rayon thread pool")?;
    let metadata_clone_dir = get_metadata_dir();
    if metadata_clone_dir.exists() {
        fs::remove_dir_all(&metadata_clone_dir)?;
    }
    clone_repo(METADATA_REPO_URL, &metadata_clone_dir)?;
    let repos_path = metadata_clone_dir.join("repos.json");
    if !repos_path.exists() {
        let repos_meta = ReposMetadata {
            next_id: 1,
            repos: std::collections::BTreeMap::new(),
        };
        save_repos_metadata(&metadata_clone_dir, &repos_meta)?;
        fs::create_dir_all(metadata_clone_dir.join("fs"))?;
        git_add_commit_push(&metadata_clone_dir, "Initialize metadata")?;
    }
    fs::remove_dir_all(&metadata_clone_dir)?;
    Ok(())
}

pub fn ls() -> Result<()> {
    ensure_tmpfs_dir()?;
    let metadata_clone_dir = get_metadata_dir();
    if metadata_clone_dir.exists() {
        fs::remove_dir_all(&metadata_clone_dir)?;
    }
    clone_repo(METADATA_REPO_URL, &metadata_clone_dir)?;
    let fs_dir = metadata_clone_dir.join("fs");
    if !fs_dir.exists() {
        println!("No files");
    } else {
        for entry in WalkDir::new(&fs_dir).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file()
                && entry.path().extension().map_or(false, |e| e == "json")
            {
                let meta: FileMetadata = serde_json::from_reader(
                    File::open(entry.path())
                        .with_context(|| format!("reading metadata {:?}", entry.path()))?,
                )?;
                let human = human_size(meta.size);
                if let Ok(rel_path) = entry.path().strip_prefix(&fs_dir) {
                    let without_ext = rel_path.with_extension("");
                    println!("{} {}", without_ext.display(), human);
                }
            }
        }
    }
    fs::remove_dir_all(&metadata_clone_dir)?;
    Ok(())
}

pub fn clean() -> Result<()> {
    let repos = list_repos()?;
    for repo in repos {
        println!("deleting repo:{}", &repo);
        delete_repo(&repo)?;
    }
    Ok(())
}
