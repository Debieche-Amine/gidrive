use anyhow::{Context, Result};
use std::path::Path;
use std::thread;
use std::time::Duration;

use crate::constants::GITHUB_USERNAME;
use crate::utils::run;

pub fn create_repo(repo_name: &str) -> Result<()> {
    let cmd = format!(
        "gh repo create {}/{} --private --confirm",
        GITHUB_USERNAME, repo_name
    );
    run(&cmd)?;
    Ok(())
}

pub fn delete_repo(repo_name: &str) -> Result<()> {
    let cmd = format!("gh repo delete {}/{} --yes", GITHUB_USERNAME, repo_name);
    run(&cmd)?;
    Ok(())
}

pub fn repo_exists(repo_name: &str) -> bool {
    let cmd = format!(
        "gh repo view {}/{} >/dev/null 2>&1",
        GITHUB_USERNAME, repo_name
    );
    run(&cmd).is_ok()
}

pub fn ssh_agent(key_path: &str) {
    std::env::set_var(
        "GIT_SSH_COMMAND",
        format!("ssh -i {} -o IdentitiesOnly=yes", key_path),
    );
}

pub fn clone_repo(url: &str, dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).context("Failed to create clone dir")?;
    let cmd = format!("git clone {} {}", url, dir.display());
    run(&cmd).context("Failed to clone repo")?;
    Ok(())
}

pub fn git_add_commit_push(dir: &Path, msg: &str) -> Result<()> {
    let cmd_add = format!("cd {} && git add .", dir.display());
    run(&cmd_add).context("Failed to git add")?;
    let cmd_commit = format!("cd {} && git commit -m \"{}\"", dir.display(), msg);
    run(&cmd_commit).context("Failed to git commit")?;
    let cmd_push = format!("cd {} && git push origin main", dir.display());
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
