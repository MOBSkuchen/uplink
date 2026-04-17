use clap::Parser;
use std::fmt;
use std::fs::{self, DirEntry, File};
use std::io::{self, Read, Write};
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::string::FromUtf8Error;
use std::thread;
use std::time::UNIX_EPOCH;

#[path = "../../fingerprint.rs"]
mod fingerprint;

const OP_UPLOAD: u8 = 0;
const OP_DOWNLOAD: u8 = 1;
const OP_REMOVE: u8 = 2;
const MAX_NAME_LEN: u32 = 512;
const MAX_PAYLOAD: u64 = 16 * 1024 * 1024 * 1024;

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

fn store_path(storage: &Path, name: &str) -> PathBuf {
    storage.join(format!("{}.tar.zst", name))
}

fn store_meta(storage: &Path, name: &str) -> Result<()> {
    let path = storage.join(format!("{}.meta", name));
    let mut file = File::options().write(true).create(true).truncate(true).open(path.clone()).map_err(|e| {Error::CreateStorage {path: path.clone(), source: e}})?;
    file.write_all(&UNIX_EPOCH.elapsed().unwrap().as_secs().to_le_bytes()).map_err(|e| {Error::CreateStorage {path, source: e}})?;
    file.flush()?;
    Ok(())
}

fn give_meta(storage: &Path, name: &str) -> Result<[u8; 8]> {
    let path = storage.join(format!("{}.meta", name));
    let mut file = File::options().read(true).open(path.clone()).map_err(|e| {Error::LoadStorage {path: path.clone(), source: e}})?;
    let mut buf = [0u8; 8];
    file.read_exact(&mut  buf).map_err(|e| {Error::LoadStorage {path: path.clone(), source: e}})?;
    Ok(buf)
}

fn handle_upload(stream: &mut TcpStream, storage: &Path) -> Result<()> {
    let name = read_name(stream)?;
    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf)?;
    let len = u64::from_le_bytes(len_buf);
    if len > MAX_PAYLOAD {
        return Err(Error::PayloadTooLarge(len));
    }

    fs::create_dir_all(storage)?;
    let tmp = storage.join(format!("{}.tar.zst.tmp", name));
    let final_path = store_path(storage, &name);

    {
        let mut file = File::create(&tmp)?;
        let mut remaining = len;
        let mut buf = vec![0u8; 64 * 1024];
        while remaining > 0 {
            let want = (buf.len() as u64).min(remaining) as usize;
            let n = stream.read(&mut buf[..want])?;
            if n == 0 {
                return Err(Error::ClientDisconnected);
            }
            file.write_all(&buf[..n])?;
            remaining -= n as u64;
        }
        file.flush()?;
    }

    fs::rename(&tmp, &final_path)?;
    stream.write_all(&[1u8])?;
    store_meta(storage, name.as_str())?;
    println!("stored '{}' ({} bytes)", name, len);
    Ok(())
}

fn handle_download(stream: &mut TcpStream, storage: &Path) -> Result<()> {
    let name = read_name(stream)?;
    let path = store_path(storage, &name);
    if !path.exists() {
        stream.write_all(&0u64.to_le_bytes())?;
        println!("missing '{}'", name);
        return Ok(());
    }
    let fs_meta = fs::metadata(&path)?;
    let len = fs_meta.len();
    stream.write_all(&len.to_le_bytes())?;
    let meta = give_meta(storage, &name)?;
    stream.write_all(&meta)?;

    let mut file = File::open(&path)?;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        stream.write_all(&buf[..n])?;
    }
    println!("served '{}' ({} bytes)", name, len);
    Ok(())
}

fn handle_remove(stream: &mut TcpStream, storage: &Path) -> Result<()> {
    let name = read_name(stream)?;
    let path = store_path(storage, &name);
    if !path.exists() {
        stream.write_all(&0u64.to_le_bytes())?;
        println!("missing '{}'", name);
        return Ok(());
    }
    let meta_path = storage.join(format!("{}.meta", name));
    fs::remove_file(path)?;
    fs::remove_file(meta_path)?;
    stream.write_all(&[1u8])?;
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
    let paths = fs::read_dir("./").unwrap().collect::<Vec<io::Result<DirEntry>>>();
    println!("{:?}", paths);
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            print_error_chain(&e);
            ExitCode::FAILURE
        }
    }
}
