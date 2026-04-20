use std::io;
use std::io::Read;
const AUTH_KEY_SIZE: usize = 1024 * 5;

pub type AuthKey = [u8; AUTH_KEY_SIZE];

pub fn load_key<F: Read>(mut f: F) -> io::Result<AuthKey> {
    let mut key = [0u8; AUTH_KEY_SIZE];
    f.read_exact(&mut key)?;
    Ok(key)
}

pub fn read_auth<R: Read>(r: &mut R) -> io::Result<AuthKey> {
    let mut key = [0u8; AUTH_KEY_SIZE];
    r.read_exact(&mut key)?;
    Ok(key)
}