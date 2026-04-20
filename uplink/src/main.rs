use std::fs::File;
use std::io;
use clap::{Parser, Subcommand};
use console::style;
use std::io::Read;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::process::ExitCode;

mod protocol;
mod fasthash;
mod auth;
mod config;
mod error;
mod output;
mod transfer;

use config::{default_cfg_path, Config};
use error::{Error, Result};
use transfer::{download, upload};
use crate::auth::save_key;

#[derive(Parser)]
#[command(name = "uplink", about = "Directory sync client", author = "MOBSkuchen")]
struct Cli {
    #[arg(short = 's', long = "server", aliases = ["host"], default_value = "127.0.0.1:4500", help = "Address of the hot server running the uplink server")]
    server: SocketAddrV4,
    #[arg(short = 'a', long = "auth-key", aliases = ["auth", "key", "k"], default_value = ".UPLINK-AUTH", help = "Path of the authentication key (must be at least 5KB)")]
    auth_key: PathBuf,
    #[arg(short = 'n', long = "no-auth", aliases = ["na", "unsafe"], default_value = "false", help = "Send no authentication to the server")]
    no_auth: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    #[command(
        name = "upload",
        about = "Upload a directory to the server"
    )]
    Upload {
        #[arg(long, short = 'n', aliases = ["title", "sign", "entry"], help = "Access name of the directory")]
        name: String,
        #[arg(long = "target", short = 't', aliases = ["dir", "upload", "path"], help = "Directory to upload")]
        dir: PathBuf
    },
    #[command(
        name = "download",
        about = "Download an entry from the server"
    )]
    Download {
        #[arg(long, short = 'n', aliases = ["title", "sign", "entry"], help = "Access name of the directory")]
        name: String,
        #[arg(long, short = 'd', aliases = ["destination", "store"], help = "Destination directory for download")]
        dest: PathBuf,
        #[arg(long, alias = "preserve", default_value = "true")]
        no_delete: bool,
    },
    #[command(
        name = "push",
        aliases = ["give", "post", "publish"],
        about = "Push an entry defined in the config to the server"
    )]
    Push {
        #[arg(long, short = 'p', aliases = ["cfg", "config"], default_value = ".UPLINK.toml", help = "Path of the config file")]
        cfg: PathBuf
    },
    #[command(
        name = "pull",
        aliases = ["get"],
        about = "Remove an entry from the server"
    )]
    Pull {
        #[arg(long, short = 'p', aliases = ["cfg", "config"], default_value = ".UPLINK.toml", help = "Path of the config file")]
        cfg: PathBuf
    },
    #[command(
        name = "init",
        aliases = ["new", "setup"],
        about = "Create a new config"
    )]
    Init {
        #[arg(long, short = 'n', aliases = ["title", "sign", "entry"], help = "Access name of the directory")]
        name: String,
        #[arg(long = "target", short = 't', aliases = ["dir", "upload", "path"], help = "Directory to upload")]
        dir: PathBuf,
        #[arg(long = "dest", short = 'd', aliases = ["destination", "store"], help = "Destination directory for download")]
        dest: Option<PathBuf>,
        #[arg(long = "cfg-path", short = 'p', aliases = ["cfg", "config"], default_value = ".UPLINK.toml", help = "Path of the config file")]
        cfg_path: PathBuf,
        #[arg(long = "no-delete", alias = "preserve", default_value = "true", help = "Don't delete local files when downloading")]
        no_delete: bool,
    },
    #[command(
        name = "remove",
        aliases = ["delete"],
        about = "Remove an entry from the server"
    )]
    Remove {
        #[arg(long, short = 'n', aliases = ["title", "sign", "entry"], help = "Access name of the directory", group = "target")]
        name: Option<String>,
        #[arg(long, short = 'p', aliases = ["cfg", "config"], default_value = ".UPLINK.toml", help = "Path of the config file", group = "target", conflicts_with = "name")]
        cfg: Option<PathBuf>,
        #[arg(long, alias = "preserve-config", default_value = "false", required = false, requires = "cfg", help = "Don't delete config when removing remote entry")]
        preserve_cfg: bool,
    },
    #[command(
        name = "gen-key",
        aliases = ["keygen", "key-gen", "create-key", "new-key"],
        about = "Generates and stores a new authentication key"
    )]
    KeyGen {
        #[arg(long, short = 'p', default_value = ".UPLINK-AUTH", help = "Path of the auth key to generate")]
        path: PathBuf
    }
}

fn init(
    server: SocketAddrV4,
    name: String,
    dir: PathBuf,
    dest: PathBuf,
    cfg_path: PathBuf,
    no_delete: bool,
    auth_key: PathBuf,
    no_auth: bool,
) -> Result<()> {
    output::step(format!("Initializing {}", style(cfg_path.display()).cyan()));
    let cfg = Config {
        name,
        dir,
        dest,
        server,
        no_delete,
        auth_key,
        no_auth
    };
    cfg.save(&cfg_path)?;
    output::info("name", style(&cfg.name).bold().yellow().to_string());
    output::info("dir", style(cfg.dir.display()).cyan().to_string());
    output::info("dest", style(cfg.dest.display()).cyan().to_string());
    output::info("server", style(cfg.server).cyan().to_string());
    output::info("no-delete (preserve)", style(cfg.no_delete).cyan().to_string());
    output::info("auth-key", style(cfg.auth_key.display()).cyan().to_string());
    output::info("no-auth", style(cfg.no_auth).cyan().to_string());
    Ok(())
}

fn push(cfg_path: PathBuf) -> Result<()> {
    let cfg = Config::load(&cfg_path)?;
    upload(cfg.server, &cfg.name, &cfg.dir, cfg.auth_key, cfg.no_auth)
}

fn pull(cfg_path: PathBuf) -> Result<()> {
    let cfg = Config::load(&cfg_path)?;
    download(cfg.server, &cfg.name, &cfg.dest, cfg.no_delete, cfg.auth_key, cfg.no_auth)
}

fn remove(
    name: Option<String>,
    server: SocketAddrV4,
    config_path: Option<PathBuf>,
    preserve_cfg: bool,
    auth_key_path: PathBuf,
    no_auth: bool,
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

    let key = if no_auth {
        [0u8; 5120]
    } else {
        let f = File::open(&auth_key_path).map_err(|source| Error::AuthKeyLoad {
            path: auth_key_path.clone(),
            source,
        })?;
        auth::load_key(f).map_err(|source| Error::AuthKeyLoad {
            path: auth_key_path.clone(),
            source,
        })?
    };
    auth::write_auth(&mut stream, &key)?;
    let mut resp = [0u8; 1];
    stream.read_exact(&mut resp)?;
    if resp[0] != 1 {
        return Err(Error::AuthFailed);
    }

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

fn gen_key(path: PathBuf) -> Result<()> {
    output::step("Generating key");
    let f = || -> io::Result<()> {
        let file = File::options().write(true).create(true).truncate(true).open(&path)?;
        save_key(file)?;
        Ok(())
    };
    f().map_err(|source| {Error::KeyGen { path: path.clone(), source }})?;
    output::info("stored key", path.as_os_str().to_str().unwrap());
    Ok(())
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Upload { name, dir } => upload(cli.server, &name, &dir, cli.auth_key, cli.no_auth),
        Cmd::Download { name, dest, no_delete } => download(cli.server, &name, &dest, no_delete, cli.auth_key, cli.no_auth),
        Cmd::Push { cfg } => push(cfg),
        Cmd::Pull { cfg } => pull(cfg),
        Cmd::Init {
            name,
            dir,
            dest,
            cfg_path,
            no_delete
        } => init(
            cli.server,
            name,
            dir.clone(),
            dest.unwrap_or(dir),
            cfg_path,
            no_delete,
            cli.auth_key,
            cli.no_auth
        ),
        Cmd::Remove {
            name,
            cfg,
            preserve_cfg,
        } => remove(name, cli.server, cfg, preserve_cfg, cli.auth_key, cli.no_auth),
        Cmd::KeyGen { path } => gen_key(path)
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
