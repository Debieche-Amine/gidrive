use anyhow::{Context, Result};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use serde_json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{self, copy, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Duration;
use walkdir::WalkDir;

pub const NUM_PUSH_THREADS: usize = 8;

const GITHUB_USERNAME: &str = "test-storage-00";
const SSH_KEY_PATH: &str = "~/.ssh/storage01";
const METADATA_REPO_URL: &str = "git@github.com:test-storage-00/metadata.git";
const TMPFS_DIR: &str = "/tmp/gidrive-fds234sf";
const CHUNK_SIZE: usize = 1024 * 1024; // 1 MB
const MAX_SIZE_PER_REPO: u64 = 10 * 1024 * 1024; // 10 MB

fn run(cmd: &str) -> io::Result<String> {
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

/// create a repo using gh
fn create_repo(repo_name: &str) -> Result<()> {
    let cmd = format!(
        "gh repo create {}/{} --private --confirm",
        GITHUB_USERNAME, repo_name
    );
    run(&cmd)?;
    Ok(())
}

/// delete a repo using gh
fn delete_repo(repo_name: &str) -> Result<()> {
    let cmd = format!("gh repo delete {}/{} --yes", GITHUB_USERNAME, repo_name);
    run(&cmd)?;
    Ok(())
}

/// check if a repo exists using gh
fn repo_exists(repo_name: &str) -> bool {
    let cmd = format!(
        "gh repo view {}/{} >/dev/null 2>&1",
        GITHUB_USERNAME, repo_name
    );
    run(&cmd).is_ok()
}

fn ssh_agent(key_path: &str) {
    std::env::set_var(
        "GIT_SSH_COMMAND",
        format!("ssh -i {} -o IdentitiesOnly=yes", key_path),
    );
}

fn clone_repo(url: &str, dir: &Path) -> Result<()> {
    fs::create_dir_all(dir).context("Failed to create clone dir")?;
    let cmd = format!("git clone {} {}", url, dir.display());
    run(&cmd).context("Failed to clone repo")?;
    Ok(())
}

fn git_add_commit_push(dir: &Path, msg: &str) -> Result<()> {
    let cmd_add = format!("cd {} && git add .", dir.display());
    run(&cmd_add).context("Failed to git add")?;

    let cmd_commit = format!("cd {} && git commit -m \"{}\"", dir.display(), msg);
    run(&cmd_commit).context("Failed to git commit");

    let cmd_push = format!("cd {} && git push origin main", dir.display(),);

    let mut backoff = 1u64;
    loop {
        match run(&cmd_push) {
            Ok(_) => break,
            Err(e) => {
                eprintln!("Push failed: {:?}. Retrying in {}s...", e, backoff);
                thread::sleep(Duration::from_secs(backoff));
                backoff = (backoff.saturating_mul(2)).min(60);
            }
        }
    }
    Ok(())
}

fn get_file_sha256(path: &Path) -> Result<String> {
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

#[derive(Serialize, Deserialize, Clone)]
struct ChunkInfo {
    repo: String,
    path: String,
    size: u64,
    index: usize,
}

#[derive(Serialize, Deserialize)]
struct FileMetadata {
    checksum: String,
    size: u64,
    chunks: Vec<ChunkInfo>,
}

#[derive(Serialize, Deserialize)]
struct RepoInfo {
    name: String,
    current_size: u64,
}

#[derive(Serialize, Deserialize)]
struct ReposMetadata {
    next_id: usize,
    repos: BTreeMap<String, RepoInfo>,
}

fn get_metadata_dir() -> PathBuf {
    PathBuf::from(TMPFS_DIR).join("metadata")
}

fn ensure_tmpfs_dir() -> Result<()> {
    fs::create_dir_all(TMPFS_DIR).context("Failed to create TMPFS_DIR")
}

fn load_repos_metadata(metadata_clone_dir: &Path) -> Result<ReposMetadata> {
    let path = metadata_clone_dir.join("repos.json");
    if path.exists() {
        let data = fs::read_to_string(&path).context("Failed to read repos.json")?;
        serde_json::from_str(&data).context("Failed to parse repos.json")
    } else {
        Ok(ReposMetadata {
            next_id: 1,
            repos: BTreeMap::new(),
        })
    }
}

fn save_repos_metadata(metadata_clone_dir: &Path, repos_meta: &ReposMetadata) -> Result<()> {
    let path = metadata_clone_dir.join("repos.json");
    let data =
        serde_json::to_string_pretty(repos_meta).context("Failed to serialize repos.json")?;
    fs::write(&path, data).context("Failed to write repos.json")
}

fn find_or_create_repo_for_chunk(
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

fn upload_chunks_to_repo(
    checksum: &str,
    repo_name: &str,
    chunk_list: &[(usize, PathBuf, String)],
) -> Result<()> {
    let repo_url = format!("git@github.com:{}/{}.git", GITHUB_USERNAME, repo_name);
    let clone_dir = PathBuf::from(TMPFS_DIR).join(repo_name);
    if clone_dir.exists() {
        fs::remove_dir_all(&clone_dir).context("Failed to remove existing clone")?;
    }
    clone_repo(&repo_url, &clone_dir)?;

    for (_index, chunk_path, dest_path) in chunk_list {
        let dest = clone_dir.join(dest_path);
        fs::copy(chunk_path, &dest).context("Failed to copy chunk to repo")?;
    }

    git_add_commit_push(
        &clone_dir,
        &format!("Add {} chunks for {}", chunk_list.len(), checksum),
    )?;

    fs::remove_dir_all(&clone_dir).context("Failed to clean up data repo clone")?;
    Ok(())
}

fn download_chunks_from_repo(
    repo_name: &str,
    chunk_list: &[(usize, String)],
    temp_dir: &Path,
) -> Result<()> {
    let repo_url = format!("git@github.com:{}/{}.git", GITHUB_USERNAME, repo_name);
    let clone_dir = PathBuf::from(TMPFS_DIR).join(format!("dl_{}", repo_name));
    if clone_dir.exists() {
        fs::remove_dir_all(&clone_dir).context("Failed to remove existing dl clone")?;
    }
    clone_repo(&repo_url, &clone_dir)?;

    for (global_i, chunk_path_str) in chunk_list {
        let src = clone_dir.join(chunk_path_str);
        let dst = temp_dir.join(format!("chunk_{}", global_i));
        fs::copy(&src, &dst).context("Failed to copy chunk from repo")?;
    }

    fs::remove_dir_all(&clone_dir).context("Failed to clean up dl repo clone")?;
    Ok(())
}

pub fn upload_file(remote: &str, local: &str) -> Result<()> {
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

    let mut repos_meta = load_repos_metadata(&metadata_clone_dir)?;

    // Pre-assign repos for all chunks (sequential)
    let mut assignments: Vec<(usize, String, u64)> = Vec::new();
    let mut remaining = file_size;
    let mut index = 0;
    while remaining > 0 {
        let chunk_size = remaining.min(CHUNK_SIZE as u64);
        let repo_name = find_or_create_repo_for_chunk(&mut repos_meta, chunk_size)?;
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

pub fn download_file(remote: &str, local: &str) -> Result<()> {
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
            copy(&mut chunk_r, &mut output).context("Failed to copy chunk to output")?;
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
            repos: BTreeMap::new(),
        };
        save_repos_metadata(&metadata_clone_dir, &repos_meta)?;
        fs::create_dir_all(metadata_clone_dir.join("fs"))?;
        git_add_commit_push(&metadata_clone_dir, "Initialize metadata")?;
    }

    fs::remove_dir_all(&metadata_clone_dir)?;
    Ok(())
}

pub fn list_metadata() -> Result<()> {
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
                if let Ok(rel_path) = entry.path().strip_prefix(&fs_dir) {
                    println!("{}", rel_path.with_extension("").display());
                }
            }
        }
    }

    fs::remove_dir_all(&metadata_clone_dir)?;
    Ok(())
}
