# GiDrive
github allows you to store files for free, but it limits it for small files only.  
The goal of this project is to study github limits and overcome them with tricks to create what it may seem like an infinite drive

## Some Results
- The number of repositories seems to be unlimited.  
- upload speed is highly throttled by github for large files.  
- download speed is fine
- github advertise to keep files under 100mb and repo size under 1GB
- git operations(clone, push, pull) seems unlimited,but git push has a weird behaviour.
- github advertise a 5,000 requests per hour api limit, (gh repo create), but i noticed that creating a lot of repo too quicly hits other limits


## How to use
This tool is only meant to be run on a linux system, but it should work anywhere where bash, git, gh are installed

for the current version, having the rust compiler is also mandatory, specially that there is no config file, u need edit the source code, for that reason I wont provide ready to use binaries.

- Create a new github account, (dont use your personal account)  
- generate an ssh key and add it to the account,  
- make sure bash,git,gh are installed.  
- run "gh auth login" to authenticate gh with the new account.  
- clone this repo.  
- Edit src/drive.rs, setup at least those 3 const variables: GITHUB_USERNAME SSH_KEY_PATH METADATA_REPO_URL

run with:
```bash
cargo run -- download remotefile localfile
cargo run -- upload remotefile localfile
cargo run -- ls
```

## 0.1
This is the first prototype, nothing but a proof of concept,  

Nothing is cached,everything is cleaned after each operation, wich make small operations expensive.  

There is no Delete operation, anything u upload stays there, (delete manually using gh repo delete)  

Performance is highly related to MAX_SIZE_PER_REPO and CHUNK_SIZE,  

There is no creating repos ahead, A large file with small MAX_SIZE_PER_REPO may hit the api limits  

## Want to help?
Anyone is welcomed to be a contributor, just text me on [facebook](https://www.facebook.com/amin.debieche.35)

