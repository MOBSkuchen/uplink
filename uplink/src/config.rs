use console::style;
use serde::{Deserialize, Serialize};
use std::fs;
use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::error::{Error, Result};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub name: String,
    pub dir: PathBuf,
    pub dest: PathBuf,
    pub server: SocketAddrV4,
    pub no_delete: bool,
    pub auth_key: PathBuf,
    pub no_auth: bool,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        crate::output::step(format!("Loading {}", style(path.display()).cyan()));
        let text = fs::read_to_string(path).map_err(|source| Error::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?;
        toml::from_str(&text).map_err(|source| Error::ConfigParse {
            path: path.to_path_buf(),
            source,
        })
    }

    pub fn save(&self, path: &Path) -> Result<()> {
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

pub fn default_cfg_path() -> PathBuf {
    PathBuf::from_str(".UPLINK.toml").unwrap()
}
