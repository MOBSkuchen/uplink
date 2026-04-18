use clap::{Parser, Subcommand};
use console::style;
use std::io::Read;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::process::ExitCode;

#[path = "../../protocol.rs"]
#[allow(unused)]
mod protocol;

#[path = "../../fasthash.rs"]
#[allow(unused)]
mod fasthash;

mod config;
mod error;
mod output;
mod transfer;

use config::{default_cfg_path, Config};
use error::{Error, Result};
use transfer::{download, upload};

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
        no_delete: bool,
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
        preserve_cfg: bool,
    },
}

fn init(
    server: SocketAddrV4,
    name: String,
    dir: PathBuf,
    dest: PathBuf,
    cfg_path: PathBuf,
    no_delete: bool,
) -> Result<()> {
    output::step(format!("Initializing {}", style(cfg_path.display()).cyan()));
    let cfg = Config {
        name,
        dir,
        dest,
        server,
        no_delete,
    };
    cfg.save(&cfg_path)?;
    output::info("name", style(&cfg.name).bold().yellow().to_string());
    output::info("dir", style(cfg.dir.display()).cyan().to_string());
    output::info("dest", style(cfg.dest.display()).cyan().to_string());
    output::info("server", style(cfg.server).cyan().to_string());
    output::info("no-delete (preserve)", style(cfg.no_delete).cyan().to_string());
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

fn remove(
    name: Option<String>,
    server: SocketAddrV4,
    config_path: Option<PathBuf>,
    preserve_cfg: bool,
) -> Result<()> {
    let (addr, name, cfg_path) = if let Some(ref cfg_path) = config_path {
        let cfg = Config::load(cfg_path)?;
        (cfg.server, cfg.name, Some(cfg_path.clone()))
    } else if let Some(name) = name {
        (server, name, None)
    } else {
        let cfg = Config::load(&default_cfg_path())?;
        (cfg.server, cfg.name, Some(default_cfg_path()))
    };

    output::step(format!(
        "Deleting '{}' from {}",
        style(&name).cyan(),
        style(server).cyan()
    ));

    use std::io::Write;
    use std::net::{SocketAddr, TcpStream};
    use protocol::OP_REMOVE;

    let mut stream = TcpStream::connect(SocketAddr::V4(addr)).map_err(|source| Error::Connect {
        addr: server.to_string(),
        source,
    })?;
    stream.write_all(&[OP_REMOVE])?;
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err(Error::InvalidName);
    }
    if bytes.len() > u32::MAX as usize {
        return Err(Error::NameTooLong);
    }
    stream.write_all(&(bytes.len() as u32).to_le_bytes())?;
    stream.write_all(bytes)?;

    let mut accept_buf = [0u8; 1];
    stream.read_exact(&mut accept_buf)?;
    match accept_buf[0] {
        0 => Err(Error::NotFound(name)),
        1 => Ok(()),
        _ => Err(Error::ServerRejected { opcode: OP_REMOVE }),
    }?;

    if !preserve_cfg && let Some(cfg) = cfg_path {
        output::info(
            "removed config",
            format!("{:?}", style(cfg.clone()).bold().yellow()),
        );
        std::fs::remove_file(cfg)?;
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
        Cmd::Init {
            name,
            dir,
            dest,
            cfg_path,
            no_delete,
        } => init(
            cli.server,
            name,
            dir.clone(),
            dest.unwrap_or(dir),
            cfg_path.unwrap_or_else(default_cfg_path),
            no_delete,
        ),
        Cmd::Remove {
            name,
            cfg,
            preserve_cfg,
        } => remove(name, cli.server, cfg, preserve_cfg),
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
