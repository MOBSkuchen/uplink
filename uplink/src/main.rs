use clap::{Parser, Subcommand};
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::io::{self, Cursor, Read, Write};
use std::net::{SocketAddr, SocketAddrV4, TcpStream};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;

const DEFAULT_CFG: &str = ".UPLINK.json";
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
}

impl Config {
    fn load(path: &Path) -> Result<Self> {
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

const OP_UPLOAD: u8 = 0;
const OP_DOWNLOAD: u8 = 1;
const ZSTD_LEVEL: i32 = 3;

fn step(msg: impl AsRef<str>) {
    println!("{}{}", style("> ").cyan().bold(), style(msg.as_ref()).bold());
}

fn info(label: &str, value: impl AsRef<str>) {
    println!("  {} {}", style(format!("{}:", label)).dim(), value.as_ref());
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
    Download { name: String, dest: PathBuf },
    Push { cfg: Option<PathBuf> },
    Pull { cfg: Option<PathBuf> },
    Init {name: String, dir: PathBuf, dest: Option<PathBuf>, cfg_path: Option<PathBuf>},
}

#[derive(Debug)]
enum Error {
    Io(io::Error),
    Connect { addr: String, source: io::Error },
    NotADirectory(PathBuf),
    InvalidName,
    NameTooLong,
    ServerRejected,
    NotFound(String),
    ShortRead { expected: u64, got: u64 },
    ProgressTemplate(indicatif::style::TemplateError),
    ConfigRead { path: PathBuf, source: io::Error },
    ConfigWrite { path: PathBuf, source: io::Error },
    ConfigParse { path: PathBuf, source: toml::de::Error },
    ConfigSerialize(toml::ser::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {}", e),
            Error::Connect { addr, source } => write!(f, "failed to connect to {} ({})", addr, source),
            Error::NotADirectory(p) => write!(f, "not a directory: {}", p.display()),
            Error::InvalidName => write!(f, "invalid name"),
            Error::NameTooLong => write!(f, "name too long"),
            Error::ServerRejected => write!(f, "server rejected upload"),
            Error::NotFound(n) => write!(f, "'{}' not found on server", n),
            Error::ShortRead { expected, got } => {
                write!(f, "short read: expected {} bytes, got {}", expected, got)
            }
            Error::ProgressTemplate(e) => write!(f, "progress bar template error: {}", e),
            Error::ConfigRead { path, source } => {
                write!(f, "failed to read config {}: {}", path.display(), source);
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

fn dir_size(dir: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        if ft.is_dir() {
            total += dir_size(&entry.path())?;
        } else if ft.is_file() {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

fn progress(len: u64, msg: &'static str, spinner: &'static str) -> Result<ProgressBar> {
    let pb = ProgressBar::new(len);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "  {spinner:.cyan} {msg:<12.bold.cyan} [{bar:32.cyan/blue}] \
                 {bytes:>10.green}/{total_bytes:.green} {percent:>3.yellow}% \
                 {bytes_per_sec:.magenta} eta {eta:.dim}",
            )?
            .tick_chars(spinner)
            .progress_chars("█▉▊▋▌▍▎▏ "),
    );
    pb.set_message(msg);
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    Ok(pb)
}

fn pack_dir(dir: &Path) -> Result<Vec<u8>> {
    if !dir.is_dir() {
        return Err(Error::NotADirectory(dir.to_path_buf()));
    }
    let total = dir_size(dir).unwrap_or(0);
    let pb = progress(total, "packing", SPINNER_PACK)?;

    let mut tar_buf = Vec::new();
    {
        let writer = pb.wrap_write(&mut tar_buf);
        let mut builder = tar::Builder::new(writer);
        builder.append_dir_all(".", dir)?;
        builder.finish()?;
    }

    pb.finish_and_clear();

    let pb = progress(tar_buf.len() as u64, "compressing", SPINNER_COMP)?;
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

fn unpack_to(dest: &Path, data: &[u8]) -> Result<()> {
    fs::create_dir_all(dest)?;

    let mut decompressed = Vec::new();
    {
        let pb = progress(data.len() as u64, "decomp", SPINNER_DECOMP)?;
        let mut decoder = zstd::Decoder::new(pb.wrap_read(data))?;
        io::copy(&mut decoder, &mut decompressed)?;
        pb.finish_and_clear();
    }

    let pb = progress(decompressed.len() as u64, "unpack", SPINNER_PACK)?;
    {
        let reader = pb.wrap_read(Cursor::new(&decompressed));
        let mut archive = tar::Archive::new(reader);
        archive.unpack(dest)?;
    }
    pb.finish_and_clear();
    Ok(())
}

fn upload(server: SocketAddrV4, name: &str, dir: &Path) -> Result<()> {
    step(format!("Packing {}", style(dir.display()).cyan()));
    let data = pack_dir(dir)?;

    step(format!("Uploading to {}", style(server).cyan()));
    let mut stream = TcpStream::connect(SocketAddr::V4(server)).map_err(|source| Error::Connect {
        addr: server.to_string(),
        source,
    })?;
    stream.write_all(&[OP_UPLOAD])?;
    write_name(&mut stream, name)?;
    stream.write_all(&(data.len() as u64).to_le_bytes())?;

    let pb = progress(data.len() as u64, "upload", SPINNER_LOAD)?;
    let mut written = 0usize;
    let chunk = 64 * 1024;
    while written < data.len() {
        let end = (written + chunk).min(data.len());
        stream.write_all(&data[written..end])?;
        written = end;
        pb.set_position(written as u64);
    }
    pb.finish_and_clear();

    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack)?;
    if ack[0] != 1 {
        return Err(Error::ServerRejected);
    }
    info(
        "uploaded",
        format!("{}", style(name).bold().yellow())
    );
    Ok(())
}

fn download(server: SocketAddrV4, name: &str, dest: &Path) -> Result<()> {
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

    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf)?;
    let len = u64::from_le_bytes(len_buf);
    if len == 0 {
        return Err(Error::NotFound(name.to_string()));
    }
    info("size", style(format_size(len)).green().bold().to_string());

    let pb = progress(len, "download", SPINNER_LOAD)?;
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
    unpack_to(dest, &data)?;
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
) -> Result<()> {
    step(format!("Initializing {}", style(cfg_path.display()).cyan()));
    let cfg = Config { name, dir, dest, server };
    cfg.save(&cfg_path)?;
    info("name", style(&cfg.name).bold().yellow().to_string());
    info("dir", style(cfg.dir.display()).cyan().to_string());
    info("dest", style(cfg.dest.display()).cyan().to_string());
    info("server", style(cfg.server).cyan().to_string());
    Ok(())
}

fn push(cfg_path: PathBuf) -> Result<()> {
    step(format!("Loading {}", style(cfg_path.display()).cyan()));
    let cfg = Config::load(&cfg_path)?;
    upload(cfg.server, &cfg.name, &cfg.dir)
}

fn pull(cfg_path: PathBuf) -> Result<()> {
    step(format!("Loading {}", style(cfg_path.display()).cyan()));
    let cfg = Config::load(&cfg_path)?;
    download(cfg.server, &cfg.name, &cfg.dest)
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Upload { name, dir } => upload(cli.server, &name, &dir),
        Cmd::Download { name, dest } => download(cli.server, &name, &dest),
        Cmd::Push { cfg } => push(cfg.unwrap_or_else(default_cfg_path)),
        Cmd::Pull { cfg } => pull(cfg.unwrap_or_else(default_cfg_path)),
        Cmd::Init { name, dir, dest, cfg_path } => init(
            cli.server,
            name,
            dir.clone(),
            dest.unwrap_or(dir),
            cfg_path.unwrap_or_else(default_cfg_path),
        ),
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
