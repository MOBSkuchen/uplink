use clap::{Parser, Subcommand};
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Formatter};
use std::fs;
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, Write};
use std::net::{SocketAddr, SocketAddrV4, TcpStream};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::time::{Duration};
use chrono::{DateTime, Utc};
use rand::seq::IndexedRandom;
use crate::protocol::{
    create_fingerprint, read_diffmap, write_dirfp, DiffMap, DirFp,
    OP_UPLOAD, OP_DOWNLOAD, OP_REMOVE,
};
use crate::fasthash::{
    CHUNK_SIZE, PART_THRESHOLD,
    hash_file, compare_hashes, chunk_len, reconstruct_file,
    write_part_path, write_hashes, read_hashes, write_needed, read_needed, read_part_path,
};
use crate::RandomMessage::{ClientProcessing, Compressing, Decompressing, Downloading, Packing, ServerProcessing, Unpacking, Uploading, WaitingOnServer};

#[path = "../../protocol.rs"]
#[allow(unused)]
mod protocol;

#[path = "../../fasthash.rs"]
#[allow(unused)]
mod fasthash;

const DEFAULT_CFG: &str = ".UPLINK.toml";
const SPINNER_LOAD: &str = "←↖↑↗→↘↓↙";
const SPINNER_COMP: &str = "┤┘┴└├┌┬┐";
const SPINNER_DECOMP: &str = "▖▘▝▗";
const SPINNER_PACK: &str = "◐◓◑◒";

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Config {
    name: String,
    dir: PathBuf,
    dest: PathBuf,
    server: SocketAddrV4,
    no_delete: bool
}

impl Config {
    fn load(path: &Path) -> Result<Self> {
        step(format!("Loading {}", style(path.display()).cyan()));
        let text = fs::read_to_string(path).map_err(|source| Error::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?;
        toml::from_str(&text).map_err(|source| Error::ConfigParse {
            path: path.to_path_buf(),
            source,
        })
    }

    fn save(&self, path: &Path) -> Result<()> {
        let abs = absolutize(path)?;
        if let Some(parent) = abs.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|source| Error::ConfigWrite {
                    path: abs.clone(),
                    source,
                })?;
            }
        }
        let text = toml::to_string_pretty(self).map_err(Error::ConfigSerialize)?;
        fs::write(&abs, text).map_err(|source| Error::ConfigWrite {
            path: abs,
            source,
        })
    }
}

fn absolutize(path: &Path) -> Result<PathBuf> {
    std::path::absolute(path).map_err(|source| Error::ConfigWrite {
        path: path.to_path_buf(),
        source,
    })
}

fn default_cfg_path() -> PathBuf {
    PathBuf::from_str(DEFAULT_CFG).unwrap()
}

const ZSTD_LEVEL: i32 = 3;

fn step(msg: impl AsRef<str>) {
    println!("{}{}", style("> ").cyan().bold(), style(msg.as_ref()).bold());
}

fn info(label: &str, value: impl AsRef<str>) {
    println!("  {} {}", style(format!("{}:", label)).dim(), value.as_ref());
}

enum RandomMessage {
    ServerProcessing,
    ClientProcessing,
    Uploading,
    Downloading,
    Packing,
    Unpacking,
    Compressing,
    Decompressing,
    WaitingOnServer,
}

impl RandomMessage {
    fn random_message(&self) -> &'static str {
        let mut rng = rand::rng();

        let choices = match self {
            ServerProcessing => &[
                "conjuring",
                "brewing",
                "percolating",
                "ruminating",
                "divining",
            ],
            ClientProcessing => &[
                "tinkering",
                "juggling",
                "doodling",
                "weaving",
                "whittling",
            ],
            Uploading => &[
                "flinging",
                "catapulting",
                "levitating",
                "ascending",
                "whooshing",
            ],
            Downloading => &[
                "summoning",
                "snatching",
                "materializing",
                "descending",
                "swooping",
            ],
            Packing => &[
                "swaddling",
                "stuffing",
                "nesting",
                "bundling",
                "tucking",
            ],
            Unpacking => &[
                "blooming",
                "hatching",
                "unfurling",
                "sprouting",
                "spilling",
            ],
            Compressing => &[
                "squeezing",
                "shrinking",
                "squashing",
                "smushing",
                "deflating",
            ],
            Decompressing => &[
                "stretching",
                "popping",
                "inflating",
                "fluffing",
                "uncoiling",
            ],
            WaitingOnServer => &[
                "snoozing",
                "daydreaming",
                "twiddling",
                "yearning",
                "pining",
            ],
        };

        choices.choose(&mut rng).copied().unwrap_or("thinking")
    }

    fn spinner(&self) -> &'static str {
        match self {
            ServerProcessing => SPINNER_LOAD,
            ClientProcessing => SPINNER_LOAD,
            Uploading => SPINNER_LOAD,
            Downloading => SPINNER_LOAD,
            Packing => SPINNER_PACK,
            Unpacking => SPINNER_PACK,
            Compressing => SPINNER_COMP,
            Decompressing => SPINNER_DECOMP,
            WaitingOnServer => SPINNER_LOAD,
        }
    }
}

#[derive(Parser)]
#[command(name = "uplink", about = "Directory sync client")]
struct Cli {
    #[arg(short, long, default_value = "127.0.0.1:4500")]
    server: SocketAddrV4,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Upload { name: String, dir: PathBuf },
    Download {
        name: String,
        dest: PathBuf,
        #[arg(long, alias = "preserve", default_value = "true")]
        no_delete: bool
    },
    Push { cfg: Option<PathBuf> },
    Pull { cfg: Option<PathBuf> },
    Init {
        name: String,
        dir: PathBuf,
        dest: Option<PathBuf>,
        cfg_path: Option<PathBuf>,
        #[arg(long, alias = "preserve", default_value = "true")]
        no_delete: bool,
    },
    Remove {
        #[arg(long, group = "target")]
        name: Option<String>,
        #[arg(long, group = "target", conflicts_with = "name")]
        cfg: Option<PathBuf>,
        #[arg(long, alias = "preserve-config", default_value = "false", required = false)]
        preserve_cfg: bool
    },
}

#[derive(Debug)]
enum Error {
    Io(io::Error),
    Connect { addr: String, source: io::Error },
    NotADirectory(PathBuf),
    InvalidName,
    NameTooLong,
    ServerRejected{ opcode: u8 },
    NotFound(String),
    ShortRead { expected: u64, got: u64 },
    ProgressTemplate(indicatif::style::TemplateError),
    ConfigRead { path: PathBuf, source: io::Error },
    ConfigWrite { path: PathBuf, source: io::Error },
    ConfigParse { path: PathBuf, source: toml::de::Error },
    ConfigSerialize(toml::ser::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {}", e),
            Error::Connect { addr, source } => write!(f, "failed to connect to {} ({})", addr, source),
            Error::NotADirectory(p) => write!(f, "not a directory: {}", p.display()),
            Error::InvalidName => write!(f, "invalid name"),
            Error::NameTooLong => write!(f, "name too long"),
            Error::ServerRejected {opcode} => {
                match *opcode {
                    OP_UPLOAD => write!(f, "server rejected upload"),
                    OP_DOWNLOAD => write!(f, "server rejected download"),
                    OP_REMOVE => write!(f, "server rejected remove"),
                    _ => unreachable!()
                }
            },
            Error::NotFound(n) => write!(f, "'{}' not found on server", n),
            Error::ShortRead { expected, got } => {
                write!(f, "short read: expected {} bytes, got {}", expected, got)
            }
            Error::ProgressTemplate(e) => write!(f, "progress bar template error: {}", e),
            Error::ConfigRead { path, source } => {
                let _ = write!(f, "failed to read config {}: {}", path.display(), source);
                write!(f, "\n {} use ´{}´ to create a config!", style("help:").bold().green(), style("uplink init [OPTIONS]").cyan())
            }
            Error::ConfigWrite { path, source } => {
                write!(f, "failed to write config {}: {}", path.display(), source)
            }
            Error::ConfigParse { path, source } => {
                write!(f, "invalid config {}: {}", path.display(), source)
            }
            Error::ConfigSerialize(e) => write!(f, "failed to serialize config: {}", e),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Connect { source, .. } => Some(source),
            Error::ProgressTemplate(e) => Some(e),
            Error::ConfigRead { source, .. } => Some(source),
            Error::ConfigWrite { source, .. } => Some(source),
            Error::ConfigParse { source, .. } => Some(source),
            Error::ConfigSerialize(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<indicatif::style::TemplateError> for Error {
    fn from(e: indicatif::style::TemplateError) -> Self {
        Error::ProgressTemplate(e)
    }
}

type Result<T> = std::result::Result<T, Error>;

fn format_size(bytes: u64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    if bytes == 0 {
        return "0 B".to_string();
    }
    let i = (bytes as f64).log(1024.0).floor() as usize;
    let i = i.min(units.len() - 1);
    let value = bytes as f64 / 1024.0_f64.powi(i as i32);
    if i == 0 {
        format!("{} {}", bytes, units[i])
    } else {
        format!("{:.2} {}", value, units[i])
    }
}

fn write_name(stream: &mut TcpStream, name: &str) -> Result<()> {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err(Error::InvalidName);
    }
    if bytes.len() > u32::MAX as usize {
        return Err(Error::NameTooLong);
    }
    stream.write_all(&(bytes.len() as u32).to_le_bytes())?;
    stream.write_all(bytes)?;
    Ok(())
}

fn progress(len: u64, kind: RandomMessage) -> Result<ProgressBar> {
    let pb = ProgressBar::new(len);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "  {spinner:.cyan} {msg:<12.bold.cyan} [{bar:32.cyan/blue}] \
                 {bytes:>10.green}/{total_bytes:.green} {percent:>3.yellow}% \
                 {bytes_per_sec:.magenta} eta {eta:.dim}",
            )?
            .tick_chars(kind.spinner())
            .progress_chars("█▉▊▋▌▍▎▏ "),
    );
    pb.set_message(kind.random_message());
    pb.enable_steady_tick(Duration::from_millis(80));
    Ok(pb)
}

fn spinner(kind: RandomMessage) -> Result<ProgressBar> {
    let pb = ProgressBar::new(1);
    pb.set_style(
        ProgressStyle::default_spinner()
            .template(
                "  {spinner:.cyan} {msg:<12.bold.cyan}",
            )?
            .tick_chars(kind.spinner())
    );
    pb.set_message(kind.random_message());
    pb.enable_steady_tick(Duration::from_millis(80));
    Ok(pb)
}

/// Identify files eligible for part transfer from the opcode-0 list.
/// Returns (part_paths, remaining_paths) where part_paths are large files
/// that exist on both sides, and remaining_paths go into the tar blob.
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

fn pack_paths(paths: &[String], dir: &Path) -> Result<Vec<u8>> {
    if !dir.is_dir() {
        return Err(Error::NotADirectory(dir.to_path_buf()));
    }

    let total = paths
        .iter()
        .filter_map(|p| fs::metadata(dir.join(p)).ok())
        .map(|m| m.len())
        .sum();

    let pb = progress(total, Packing)?;

    let mut tar_buf = Vec::new();
    {
        let writer = pb.wrap_write(&mut tar_buf);
        let mut builder = tar::Builder::new(writer);
        for path in paths {
            let full_path = dir.join(path);
            builder.append_path_with_name(&full_path, path)?;
        }
        builder.finish()?;
    }

    pb.finish_and_clear();

    let pb = progress(tar_buf.len() as u64, Compressing)?;
    let mut compressed = Vec::new();
    {
        let mut encoder = zstd::Encoder::new(&mut compressed, ZSTD_LEVEL)?;
        let mut reader = pb.wrap_read(Cursor::new(&tar_buf));
        io::copy(&mut reader, &mut encoder)?;
        encoder.finish()?;
    }
    pb.finish_and_clear();

    let ratio = (compressed.len() as f64 / tar_buf.len() as f64) * 100f64 - 100f64;
    info(
        "compressed",
        format!(
            "{} {}",
            style(format_size(compressed.len() as u64)).green().bold(),
            style(format!("({:+.0}%)", ratio)).yellow()
        ),
    );
    Ok(compressed)
}

fn apply_diff(dest: &Path, dm: &DiffMap, data: &[u8], no_delete: bool) -> Result<()> {
    fs::create_dir_all(dest)?;

    // Create dirs
    if let Some(dirs) = dm.get(&2) {
        for dir in dirs {
            fs::create_dir_all(dest.join(dir))?;
        }
    }

    // Decompress and unpack uploads
    if dm.get(&0).is_some_and(|v| !v.is_empty()) && !data.is_empty() {
        let mut decompressed = Vec::new();
        {
            let pb = progress(data.len() as u64, Decompressing)?;
            let mut decoder = zstd::Decoder::new(pb.wrap_read(data))?;
            io::copy(&mut decoder, &mut decompressed)?;
            pb.finish_and_clear();
        }

        let pb = progress(decompressed.len() as u64, Unpacking)?;
        {
            let reader = pb.wrap_read(Cursor::new(&decompressed));
            let mut archive = tar::Archive::new(reader);
            archive.unpack(dest)?;
        }
        pb.finish_and_clear();
    }

    // Delete files/dirs
    if !no_delete && let Some(deletions) = dm.get(&1) {
        for path in deletions {
            let full_path = dest.join(path);
            if full_path.is_dir() {
                fs::remove_dir_all(&full_path)?;
            } else if full_path.exists() {
                fs::remove_file(&full_path)?;
            }
        }
    }

    Ok(())
}

fn send_blob(stream: &mut TcpStream, data: &[u8], server: SocketAddrV4) -> Result<()> {
    stream.write_all(&(data.len() as u64).to_le_bytes())?;
    step(format!("Uploading to {}", style(server).cyan()));
    let pb = progress(data.len() as u64, Uploading)?;
    let mut written = 0usize;
    let chunk = 64 * 1024;
    while written < data.len() {
        let end = (written + chunk).min(data.len());
        stream.write_all(&data[written..end])?;
        written = end;
        pb.set_position(written as u64);
    }
    pb.finish_and_clear();
    Ok(())
}

/// Client-side part transfer: send hashes, receive needed indices, send chunks.
fn send_part_transfer(
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

        info(
            "part-sync",
            format!(
                "{} ({}/{} chunks)",
                style(path).cyan(),
                style(needed.len()).yellow().bold(),
                style(hashes.len()).dim(),
            ),
        );

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

/// Client-side part receive: read hashes from server, compare, send needed indices, receive chunks.
fn recv_part_transfer(
    stream: &mut TcpStream,
    dest: &Path,
) -> Result<()> {
    let mut u32_buf = [0u8; 4];
    stream.read_exact(&mut u32_buf)?;
    let part_count = u32::from_le_bytes(u32_buf) as usize;

    for _ in 0..part_count {
        let path = read_part_path(stream)?;
        let (new_file_len, source_hashes) = read_hashes(stream)?;

        let local_path = dest.join(&path);
        let local_hashes = if local_path.exists() {
            hash_file(&local_path)?.1
        } else {
            Vec::new()
        };

        let needed = compare_hashes(&source_hashes, &local_hashes);

        info(
            "part-sync",
            format!(
                "{} ({}/{} chunks)",
                style(&path).cyan(),
                style(needed.len()).yellow().bold(),
                style(source_hashes.len()).dim(),
            ),
        );

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

fn upload(server: SocketAddrV4, name: &str, dir: &Path) -> Result<()> {
    let mut stream = TcpStream::connect(SocketAddr::V4(server)).map_err(|source| Error::Connect {
        addr: server.to_string(),
        source,
    })?;
    stream.write_all(&[OP_UPLOAD])?;
    write_name(&mut stream, name)?;
    let fp = create_fingerprint(dir)?;
    write_dirfp(&mut stream, &fp)?;

    let sp = spinner(WaitingOnServer)?;
    let dm = read_diffmap(&mut stream)?;

    // Read server's fingerprint so we can identify part-transfer candidates
    let server_fp = protocol::read_dirfp(&mut stream)?;

    if dm.is_empty() {
        sp.finish_and_clear();
        step("Already up to date");
        let mut ack = [0u8; 1];
        stream.read_exact(&mut ack)?;
        return Ok(());
    }

    let (part_paths, remaining_paths) = split_part_candidates(&dm, &fp, &server_fp);

    sp.finish_and_clear();
    // Phase 1: Part transfer for large modified files
    if !part_paths.is_empty() {
        step(format!("Part-syncing {} large file(s)", style(part_paths.len()).cyan().bold()));
        send_part_transfer(&mut stream, dir, &part_paths)?;
    } else {
        stream.write_all(&0u32.to_le_bytes())?;
    }

    // Phase 2: Tar transfer for remaining files
    if !remaining_paths.is_empty() {
        step(format!("Packing {}", style(dir.display()).cyan()));
        let data = pack_paths(&remaining_paths, dir)?;
        send_blob(&mut stream, &data, server)?;
    } else {
        stream.write_all(&0u64.to_le_bytes())?;
    }

    let sp = spinner(ServerProcessing)?;
    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack)?;
    sp.finish_and_clear();
    if ack[0] != 1 {
        return Err(Error::ServerRejected {opcode: OP_UPLOAD});
    }
    info(
        "uploaded",
        format!("{}", style(name).bold().yellow())
    );
    Ok(())
}

struct Metadata {
    time: u64
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let datetime: DateTime<Utc> = DateTime::from_timestamp(self.time as i64, 0).unwrap();
        write!(f, "{}", datetime.format("%d.%m.%Y %H:%M:%S"))
    }
}

fn read_meta(stream: &mut TcpStream) -> Result<Metadata> {
    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf)?;
    let time = u64::from_le_bytes(len_buf);
    let meta = Metadata {time};
    Ok(meta)
}

fn download(server: SocketAddrV4, name: &str, dest: &Path, no_delete: bool) -> Result<()> {
    step(format!(
        "Requesting '{}' from {}",
        style(name).cyan(),
        style(server).cyan()
    ));
    let mut stream = TcpStream::connect(SocketAddr::V4(server)).map_err(|source| Error::Connect {
        addr: server.to_string(),
        source,
    })?;
    stream.write_all(&[OP_DOWNLOAD])?;
    write_name(&mut stream, name)?;

    let local_fp = if dest.is_dir() {
        create_fingerprint(dest)?
    } else {
        DirFp::new()
    };
    write_dirfp(&mut stream, &local_fp)?;

    let sp = spinner(WaitingOnServer)?;
    let mut status = [0u8; 1];
    stream.read_exact(&mut status)?;
    if status[0] == 0 {
        sp.finish_and_clear();
        return Err(Error::NotFound(name.to_string()));
    }

    let dm = read_diffmap(&mut stream)?;
    sp.finish_and_clear();

    let meta = read_meta(&mut stream)?;
    info("updated", style(meta).green().bold().to_string());
    let spinner = spinner(ClientProcessing)?;

    if dm.is_empty() {
        spinner.finish_and_clear();
        step("Already up to date");
        return Ok(());
    }

    // Phase 1: Part transfer for large modified files
    recv_part_transfer(&mut stream, dest)?;

    // Phase 2: Tar transfer for remaining files
    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf)?;
    let len = u64::from_le_bytes(len_buf);

    spinner.finish_and_clear();
    if len > 0 {
        info("patch size", style(format_size(len)).green().bold().to_string());

        let pb = progress(len, Downloading)?;
        let mut data = Vec::with_capacity(len as usize);
        {
            let mut reader = pb.wrap_read(stream.take(len));
            reader.read_to_end(&mut data)?;
        }
        pb.finish_and_clear();

        if (data.len() as u64) != len {
            return Err(Error::ShortRead {
                expected: len,
                got: data.len() as u64,
            });
        }

        step(format!("Unpacking into {}", style(dest.display()).cyan()));
        apply_diff(dest, &dm, &data, no_delete)?;
    } else {
        // May still have deletes/mkdirs
        let has_deletes = dm.get(&1).is_some_and(|v| !v.is_empty());
        let has_mkdirs = dm.get(&2).is_some_and(|v| !v.is_empty());
        if has_deletes || has_mkdirs {
            step(format!("Applying changes to {}", style(dest.display()).cyan()));
            apply_diff(dest, &dm, &[], no_delete)?;
        }
    }

    info(
        "downloaded",
        format!("{}", style(name).bold().yellow())
    );
    Ok(())
}

fn init(
    server: SocketAddrV4,
    name: String,
    dir: PathBuf,
    dest: PathBuf,
    cfg_path: PathBuf,
    no_delete: bool
) -> Result<()> {
    step(format!("Initializing {}", style(cfg_path.display()).cyan()));
    let cfg = Config { name, dir, dest, server, no_delete };
    cfg.save(&cfg_path)?;
    info("name", style(&cfg.name).bold().yellow().to_string());
    info("dir", style(cfg.dir.display()).cyan().to_string());
    info("dest", style(cfg.dest.display()).cyan().to_string());
    info("server", style(cfg.server).cyan().to_string());
    info("no-delete (preserve)", style(cfg.no_delete).cyan().to_string());
    Ok(())
}

fn push(cfg_path: PathBuf) -> Result<()> {
    let cfg = Config::load(&cfg_path)?;
    upload(cfg.server, &cfg.name, &cfg.dir)
}

fn pull(cfg_path: PathBuf) -> Result<()> {
    let cfg = Config::load(&cfg_path)?;
    download(cfg.server, &cfg.name, &cfg.dest, cfg.no_delete)
}

fn remove(name: Option<String>, server: SocketAddrV4, config_path: Option<PathBuf>, preserve_cfg: bool) -> Result<()> {
    let (addr, name, cfg_path) = if let Some(ref cfg_path) = config_path {
        let cfg = Config::load(cfg_path)?;
        (cfg.server, cfg.name, Some(cfg_path))
    } else if let Some(name) = name {
        (server, name, None)
    } else {
        let cfg = Config::load(&default_cfg_path())?;
        (cfg.server, cfg.name, Some(&default_cfg_path()))
    };

    step(format!(
        "Deleting '{}' from {}",
        style(&name).cyan(),
        style(server).cyan()
    ));

    let mut stream = TcpStream::connect(SocketAddr::V4(addr)).map_err(|source| Error::Connect {
        addr: server.to_string(),
        source,
    })?;
    stream.write_all(&[OP_REMOVE])?;
    write_name(&mut stream, &name)?;
    let mut accept_buf = [0u8; 1];
    stream.read_exact(&mut accept_buf)?;
    match accept_buf[0] {
        0 => Err(Error::NotFound(name)),
        1 => Ok(()),
        _ => Err(Error::ServerRejected {opcode: OP_REMOVE})
    }?;

    if !preserve_cfg && let Some(cfg) = cfg_path {
        info(
            "removed config",
            format!("{:?}", style(cfg.clone()).bold().yellow())
        );
        fs::remove_file(cfg)?;
    }

    println!("  {}", style("done").green().bold());

    Ok(())
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Upload { name, dir } => upload(cli.server, &name, &dir),
        Cmd::Download { name, dest, no_delete } => download(cli.server, &name, &dest, no_delete),
        Cmd::Push { cfg } => push(cfg.unwrap_or_else(default_cfg_path)),
        Cmd::Pull { cfg } => pull(cfg.unwrap_or_else(default_cfg_path)),
        Cmd::Init { name, dir, dest, cfg_path, no_delete } => init(
            cli.server,
            name,
            dir.clone(),
            dest.unwrap_or(dir),
            cfg_path.unwrap_or_else(default_cfg_path),
            no_delete
        ),
        Cmd::Remove { name, cfg, preserve_cfg } =>
            remove(name, cli.server, cfg, preserve_cfg),
    }
}

fn print_error_chain(err: &Error) {
    eprintln!(
        "{} {}",
        style("error:").red().bold(),
        style(err).red()
    );
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
