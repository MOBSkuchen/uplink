use std::fmt;
use std::io;
use std::path::PathBuf;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Connect { addr: String, source: io::Error },
    NotADirectory(PathBuf),
    InvalidName,
    NameTooLong,
    ServerRejected { opcode: u8 },
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
            Error::ServerRejected { opcode } => {
                match *opcode {
                    crate::protocol::OP_UPLOAD => write!(f, "server rejected upload"),
                    crate::protocol::OP_DOWNLOAD => write!(f, "server rejected download"),
                    crate::protocol::OP_REMOVE => write!(f, "server rejected remove"),
                    _ => unreachable!(),
                }
            }
            Error::NotFound(n) => write!(f, "'{}' not found on server", n),
            Error::ShortRead { expected, got } => {
                write!(f, "short read: expected {} bytes, got {}", expected, got)
            }
            Error::ProgressTemplate(e) => write!(f, "progress bar template error: {}", e),
            Error::ConfigRead { path, source } => {
                let _ = write!(f, "failed to read config {}: {}", path.display(), source);
                let _ = write!(f, "\n {} use ´{}´ to create a config!",
                    console::style("help:").bold().green(),
                    console::style("uplink init [OPTIONS]").cyan());
                Ok(())
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

pub type Result<T> = std::result::Result<T, Error>;
