use std::io::{Read, Write};
use std::io;
const AUTH_KEY_SIZE: usize = 1024 * 5;

pub type AuthKey = [u8; AUTH_KEY_SIZE];

fn gen_key() -> AuthKey {
    let mut key = [0u8; AUTH_KEY_SIZE];
    for b in key.iter_mut() {
        *b = rand::random();
    }
    key
}

pub fn save_key<F: Write>(mut f: F) -> io::Result<AuthKey> {
    let key = gen_key();
    f.write_all(&key)?;
    Ok(key)
}

pub fn load_key<F: Read>(mut f: F) -> io::Result<AuthKey> {
    let mut key = [0u8; AUTH_KEY_SIZE];
    f.read_exact(&mut key)?;
    Ok(key)
}

pub fn write_auth<W: Write>(w: &mut W, key: &AuthKey) -> io::Result<()> {
    w.write_all(key)
}
