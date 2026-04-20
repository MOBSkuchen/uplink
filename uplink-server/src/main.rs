use clap::Parser;
use std::fs::{self, File};
use std::io::{self, Cursor, Read, Write};
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::thread;
use std::time::UNIX_EPOCH;

mod protocol;
mod fasthash;
mod auth;
mod error;
mod transfer;

use crate::error::{Error, Result};
use crate::protocol::{
    OP_UPLOAD, OP_DOWNLOAD, OP_REMOVE, MAX_NAME_LEN,
    DirFp, diff, read_dirfp, write_dirfp, write_diffmap,
};

#[derive(Parser)]
#[command(name = "uplink-server", about = "Uplink directory sync server")]
struct Cli {
    #[arg(short, long, default_value = "0.0.0.0:4500")]
    bind: SocketAddrV4,
    #[arg(short, long, default_value = "./uplink-store")]
    storage: PathBuf,
    #[arg(short = 'a', long = "auth-key", aliases = ["auth", "key", "k"], default_value = ".UPLINK-AUTH")]
    auth_key: Option<PathBuf>,
}


fn read_name(stream: &mut TcpStream) -> Result<String> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf);
    if len == 0 || len > MAX_NAME_LEN {
        return Err(Error::InvalidNameLength(len));
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf)?;
    let s = String::from_utf8(buf)?;
    if s.contains('/') || s.contains('\\') || s.contains("..") {
        return Err(Error::IllegalName(s));
    }
    Ok(s)
}

// Storage layout:
//   storage/{name}/       — unpacked directory tree
//   storage/{name}.fp     — serialized DirFp
//   storage/{name}.meta   — timestamp (8 bytes LE)

fn store_dir(storage: &Path, name: &str) -> PathBuf {
    storage.join(name)
}

fn store_fp_path(storage: &Path, name: &str) -> PathBuf {
    storage.join(format!("{}.fp", name))
}

fn load_stored_fp(storage: &Path, name: &str) -> Result<DirFp> {
    let path = store_fp_path(storage, name);
    if !path.exists() {
        return Ok(DirFp::new());
    }
    let data = fs::read(&path).map_err(|e| Error::LoadStorage { path: path.clone(), source: e })?;
    let mut cursor = Cursor::new(data);
    read_dirfp(&mut cursor).map_err(Error::Io)
}

fn save_stored_fp(storage: &Path, name: &str, fp: &DirFp) -> Result<()> {
    let path = store_fp_path(storage, name);
    let mut file = File::create(&path).map_err(|e| Error::CreateStorage { path: path.clone(), source: e })?;
    write_dirfp(&mut file, fp)?;
    file.flush()?;
    Ok(())
}

fn store_meta(storage: &Path, name: &str) -> Result<()> {
    let path = storage.join(format!("{}.meta", name));
    let mut file = File::options().write(true).create(true).truncate(true).open(path.clone())
        .map_err(|e| Error::CreateStorage { path: path.clone(), source: e })?;
    file.write_all(&UNIX_EPOCH.elapsed().unwrap().as_secs().to_le_bytes())
        .map_err(|e| Error::CreateStorage { path, source: e })?;
    file.flush()?;
    Ok(())
}

fn give_meta(storage: &Path, name: &str) -> Result<[u8; 8]> {
    let path = storage.join(format!("{}.meta", name));
    let mut file = File::options().read(true).open(path.clone())
        .map_err(|e| Error::LoadStorage { path: path.clone(), source: e })?;
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).map_err(|e| Error::LoadStorage { path, source: e })?;
    Ok(buf)
}


fn handle_upload(stream: &mut TcpStream, storage: &Path) -> Result<()> {
    let name = read_name(stream)?;
    let client_fp = read_dirfp(stream)?;
    let server_fp = load_stored_fp(storage, &name)?;
    let dm = diff(&client_fp, &server_fp);

    write_diffmap(stream, &dm)?;
    // Send server_fp so client can identify part-transfer candidates
    write_dirfp(stream, &server_fp)?;

    if dm.is_empty() {
        stream.write_all(&[1u8])?;
        println!("stored '{}' (up to date)", name);
        return Ok(());
    }

    let dir = store_dir(storage, &name);
    fs::create_dir_all(&dir)?;

    if let Some(dirs) = dm.get(&2) {
        for d in dirs {
            fs::create_dir_all(dir.join(d))?;
        }
    }

    // 1: Part transfer for large modified files
    transfer::recv_part_transfer_server(stream, &dir)?;

    // 2: Tar transfer for remaining files
    transfer::recv_blob(stream, &dir)?;

    if let Some(deletions) = dm.get(&1) {
        for path in deletions {
            let full = dir.join(path);
            if full.is_dir() {
                let _ = fs::remove_dir_all(&full);
            } else if full.exists() {
                let _ = fs::remove_file(&full);
            }
        }
    }

    save_stored_fp(storage, &name, &client_fp)?;
    store_meta(storage, &name)?;
    stream.write_all(&[1u8])?;
    println!("stored '{}'", name);
    Ok(())
}

fn handle_download(stream: &mut TcpStream, storage: &Path) -> Result<()> {
    let name = read_name(stream)?;
    let client_fp = read_dirfp(stream)?;

    let dir = store_dir(storage, &name);
    if !dir.exists() || !dir.is_dir() {
        stream.write_all(&[0u8])?;
        println!("missing '{}'", name);
        return Ok(());
    }
    stream.write_all(&[1u8])?;

    let server_fp = load_stored_fp(storage, &name)?;
    let dm = diff(&server_fp, &client_fp);

    write_diffmap(stream, &dm)?;

    let meta = give_meta(storage, &name)?;
    stream.write_all(&meta)?;

    if dm.is_empty() {
        println!("served '{}' (up to date)", name);
        return Ok(());
    }

    let (part_paths, remaining_paths) = transfer::split_part_candidates(&dm, &server_fp, &client_fp);

    // 1: Part transfer
    transfer::send_part_transfer_server(stream, &dir, &part_paths)?;

    // 2: Tar transfer for remaining files
    if !remaining_paths.is_empty() {
        let mut tar_buf = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_buf);
            for path in &remaining_paths {
                let full = dir.join(path);
                builder.append_path_with_name(&full, path)?;
            }
            builder.finish()?;
        }

        let mut compressed = Vec::new();
        let mut encoder = zstd::Encoder::new(&mut compressed, 3)?;
        io::copy(&mut Cursor::new(&tar_buf), &mut encoder)?;
        encoder.finish()?;

        let len = compressed.len() as u64;
        stream.write_all(&len.to_le_bytes())?;
        stream.write_all(&compressed)?;
        println!("served '{}' ({} bytes compressed + {} part-synced)", name, len, part_paths.len());
    } else {
        stream.write_all(&0u64.to_le_bytes())?;
        println!("served '{}' ({} part-synced, no bulk)", name, part_paths.len());
    }

    Ok(())
}

fn handle_remove(stream: &mut TcpStream, storage: &Path) -> Result<()> {
    let name = read_name(stream)?;
    let dir = store_dir(storage, &name);
    if !dir.exists() {
        stream.write_all(&[0u8])?;
        println!("missing '{}'", name);
        return Ok(());
    }
    let meta_path = storage.join(format!("{}.meta", name));
    let fp_path = store_fp_path(storage, &name);
    fs::remove_dir_all(&dir)?;
    let _ = fs::remove_file(&meta_path);
    let _ = fs::remove_file(&fp_path);
    stream.write_all(&[1u8])?;
    println!("removed '{}'", name);
    Ok(())
}

fn handle_client(mut stream: TcpStream, storage: PathBuf, server_key: Option<auth::AuthKey>) -> Result<()> {
    let mut op = [0u8; 1];
    stream.read_exact(&mut op)?;

    let client_key = auth::read_auth(&mut stream)?;
    if let Some(expected) = server_key {
        if client_key != expected {
            stream.write_all(&[0u8])?;
            return Err(Error::AuthFailed);
        }
    }
    stream.write_all(&[1u8])?;

    match op[0] {
        OP_UPLOAD => handle_upload(&mut stream, &storage),
        OP_DOWNLOAD => handle_download(&mut stream, &storage),
        OP_REMOVE => handle_remove(&mut stream, &storage),
        other => Err(Error::UnknownOp(other)),
    }
}

fn print_error_chain(err: &Error) {
    eprintln!("error: {}", err);
    let mut src: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(err);
    while let Some(e) = src {
        eprintln!("  caused by: {}", e);
        src = e.source();
    }
}

fn load_server_key(path: &PathBuf) -> Result<auth::AuthKey> {
    let f = File::open(path).map_err(|e| Error::LoadStorage { path: path.clone(), source: e })?;
    auth::load_key(f).map_err(|e| Error::LoadStorage { path: path.clone(), source: e })
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    fs::create_dir_all(&cli.storage).map_err(|source| Error::CreateStorage {
        path: cli.storage.clone(),
        source,
    })?;

    let server_key = cli.auth_key.as_ref().map(load_server_key).transpose()?;

    let listener = TcpListener::bind(cli.bind).map_err(|source| Error::Bind {
        addr: cli.bind.to_string(),
        source,
    })?;
    println!(
        "uplink-server listening on {} (storage: {}, auth: {})",
        cli.bind,
        cli.storage.display(),
        if server_key.is_some() { "enabled" } else { "disabled" }
    );

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let storage = cli.storage.clone();
                let key = server_key;
                thread::spawn(move || {
                    let peer = stream.peer_addr().ok();
                    if let Err(e) = handle_client(stream, storage, key) {
                        eprint!("client {:?} ", peer);
                        print_error_chain(&e);
                    }
                });
            }
            Err(e) => eprintln!("accept error: {}", e),
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            print_error_chain(&e);
            ExitCode::FAILURE
        }
    }
}
