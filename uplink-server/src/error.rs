use std::fmt;
use std::io;
use std::path::PathBuf;
use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum Error {
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
    AuthFailed,
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
            Error::AuthFailed => write!(f, "authentication failed"),
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

pub type Result<T> = std::result::Result<T, Error>;
