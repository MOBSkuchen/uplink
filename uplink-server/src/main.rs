use clap::Parser;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Cursor, Read, Seek, Write};
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::string::FromUtf8Error;
use std::thread;
use std::time::UNIX_EPOCH;

#[path = "../../protocol.rs"]
#[allow(unused)]
mod protocol;

#[path = "../../fasthash.rs"]
#[allow(unused)]
mod fasthash;

use crate::protocol::{
    OP_UPLOAD, OP_DOWNLOAD, OP_REMOVE, MAX_NAME_LEN, MAX_PAYLOAD,
    DirFp, DiffMap, diff, read_dirfp, write_dirfp, write_diffmap,
};
use crate::fasthash::{
    CHUNK_SIZE, PART_THRESHOLD,
    hash_file, compare_hashes, chunk_len, reconstruct_file,
    write_part_path, write_hashes, read_hashes, write_needed, read_needed, read_part_path,
};

#[derive(Parser)]
#[command(name = "uplink-server", about = "Uplink directory sync server")]
struct Cli {
    #[arg(short, long, default_value = "0.0.0.0:4500")]
    bind: SocketAddrV4,
    #[arg(short, long, default_value = "./uplink-store")]
    storage: PathBuf,
}

#[derive(Debug)]
enum Error {
    Io(io::Error),
    Bind { addr: String, source: io::Error },
    CreateStorage { path: PathBuf, source: io::Error },
    LoadStorage { path: PathBuf, source: io::Error },
    InvalidNameLength(u32),
    IllegalName(String),
    NameNotUtf8(FromUtf8Error),
    PayloadTooLarge(u64),
    ClientDisconnected,
    UnknownOp(u8),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {}", e),
            Error::Bind { addr, source } => write!(f, "failed to bind {}: {}", addr, source),
            Error::CreateStorage { path, source } => {
                write!(f, "failed to create storage {}: {}", path.display(), source)
            }
            Error::InvalidNameLength(n) => write!(f, "invalid name length {}", n),
            Error::IllegalName(s) => write!(f, "illegal name '{}'", s),
            Error::NameNotUtf8(e) => write!(f, "name not valid utf-8: {}", e),
            Error::PayloadTooLarge(n) => write!(f, "payload too large: {} bytes", n),
            Error::ClientDisconnected => write!(f, "client disconnected mid-transfer"),
            Error::UnknownOp(b) => write!(f, "unknown op 0x{:02x}", b),
            Error::LoadStorage { path, source } => {
                write!(f, "failed to load storage meta {}: {}", path.display(), source)
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Bind { source, .. } => Some(source),
            Error::CreateStorage { source, .. } => Some(source),
            Error::NameNotUtf8(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Self {
        Error::NameNotUtf8(e)
    }
}

type Result<T> = std::result::Result<T, Error>;

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

fn split_part_candidates(
    dm: &DiffMap,
    source_fp: &DirFp,
    other_fp: &DirFp,
) -> (Vec<String>, Vec<String>) {
    let mut part_paths = Vec::new();
    let mut remaining = Vec::new();

    if let Some(uploads) = dm.get(&0) {
        for path in uploads {
            let in_other = other_fp.contains_key(path);
            let source_size = source_fp.get(path).map(|(_, sz, _)| *sz).unwrap_or(0);
            if in_other && source_size >= PART_THRESHOLD {
                part_paths.push(path.clone());
            } else {
                remaining.push(path.clone());
            }
        }
    }

    part_paths.sort();
    remaining.sort();
    (part_paths, remaining)
}

/// Server-side: receive part transfer from client (upload scenario).
/// Client sends hashes, server compares, sends needed indices, receives chunks.
fn recv_part_transfer_server(stream: &mut TcpStream, dir: &Path) -> Result<()> {
    let mut u32_buf = [0u8; 4];
    stream.read_exact(&mut u32_buf)?;
    let part_count = u32::from_le_bytes(u32_buf) as usize;

    for _ in 0..part_count {
        let path = read_part_path(stream)?;
        let (new_file_len, source_hashes) = read_hashes(stream)?;

        let local_path = dir.join(&path);
        let local_hashes = if local_path.exists() {
            hash_file(&local_path)?.1
        } else {
            Vec::new()
        };

        let needed = compare_hashes(&source_hashes, &local_hashes);
        println!("  part-sync '{}': {}/{} chunks needed", path, needed.len(), source_hashes.len());
        write_needed(stream, &needed)?;

        let mut chunk_data = Vec::with_capacity(needed.len());
        for &idx in &needed {
            let clen = chunk_len(idx, new_file_len);
            let mut buf = vec![0u8; clen];
            stream.read_exact(&mut buf)?;
            chunk_data.push(buf);
        }

        let reconstructed = reconstruct_file(&local_path, new_file_len, &needed, &chunk_data)?;
        if let Some(parent) = local_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&local_path, &reconstructed)?;
    }
    Ok(())
}

/// Server-side: send part transfer to client (download scenario).
/// Server sends hashes, client compares, sends needed indices, server sends chunks.
fn send_part_transfer_server(
    stream: &mut TcpStream,
    dir: &Path,
    part_paths: &[String],
) -> Result<()> {
    stream.write_all(&(part_paths.len() as u32).to_le_bytes())?;

    for path in part_paths {
        let full = dir.join(path);
        let (file_len, hashes) = hash_file(&full)?;

        write_part_path(stream, path)?;
        write_hashes(stream, file_len, &hashes)?;

        let needed = read_needed(stream)?;
        println!("  part-sync '{}': {}/{} chunks requested", path, needed.len(), hashes.len());

        for &idx in &needed {
            let clen = chunk_len(idx, file_len);
            let start = idx as u64 * CHUNK_SIZE as u64;
            let mut buf = vec![0u8; clen];
            let mut f = File::open(&full)?;
            f.seek(io::SeekFrom::Start(start))?;
            f.read_exact(&mut buf)?;
            stream.write_all(&buf)?;
        }
    }
    Ok(())
}

fn recv_blob(stream: &mut TcpStream, dir: &Path) -> Result<()> {
    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf)?;
    let len = u64::from_le_bytes(len_buf);
    if len == 0 { return Ok(()); }
    if len > MAX_PAYLOAD {
        return Err(Error::PayloadTooLarge(len));
    }

    let mut compressed = vec![0u8; len as usize];
    let mut received = 0usize;
    while received < len as usize {
        let n = stream.read(&mut compressed[received..])?;
        if n == 0 {
            return Err(Error::ClientDisconnected);
        }
        received += n;
    }

    let mut decompressed = Vec::new();
    let mut decoder = zstd::Decoder::new(Cursor::new(&compressed))?;
    io::copy(&mut decoder, &mut decompressed)?;

    let mut archive = tar::Archive::new(Cursor::new(&decompressed));
    archive.unpack(dir)?;
    println!("  received {} bytes compressed", len);
    Ok(())
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

    // Create dirs (opcode 2)
    if let Some(dirs) = dm.get(&2) {
        for d in dirs {
            fs::create_dir_all(dir.join(d))?;
        }
    }

    // Phase 1: Part transfer for large modified files
    recv_part_transfer_server(stream, &dir)?;

    // Phase 2: Tar transfer for remaining files
    recv_blob(stream, &dir)?;

    // Apply deletes (opcode 1)
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

    // After applying all diffs, storage matches client
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

    // Split opcode-0 into part-transfer and bulk
    let (part_paths, remaining_paths) = split_part_candidates(&dm, &server_fp, &client_fp);

    // Phase 1: Part transfer
    send_part_transfer_server(stream, &dir, &part_paths)?;

    // Phase 2: Tar transfer for remaining files
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

fn handle_client(mut stream: TcpStream, storage: PathBuf) -> Result<()> {
    let mut op = [0u8; 1];
    stream.read_exact(&mut op)?;
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

fn run() -> Result<()> {
    let cli = Cli::parse();
    fs::create_dir_all(&cli.storage).map_err(|source| Error::CreateStorage {
        path: cli.storage.clone(),
        source,
    })?;
    let listener = TcpListener::bind(cli.bind).map_err(|source| Error::Bind {
        addr: cli.bind.to_string(),
        source,
    })?;
    println!(
        "uplink-server listening on {} (storage: {})",
        cli.bind,
        cli.storage.display()
    );

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let storage = cli.storage.clone();
                thread::spawn(move || {
                    let peer = stream.peer_addr().ok();
                    if let Err(e) = handle_client(stream, storage) {
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
