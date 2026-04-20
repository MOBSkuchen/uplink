use std::{fs, io};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::time::UNIX_EPOCH;

// Shared constants
pub const OP_UPLOAD: u8 = 0;
pub const OP_DOWNLOAD: u8 = 1;
pub const OP_REMOVE: u8 = 2;

/*
                                    Fingerprint
                    is file, (size, modification date)
*/
pub type DirFp = HashMap<String, (bool, u64, u64)>;

pub fn create_fingerprint(p: &Path) -> io::Result<DirFp> {
    let mut fp = DirFp::new();
    walk_dir(p, p, &mut fp)?;
    Ok(fp)
}

fn walk_dir(root: &Path, current: &Path, fp: &mut DirFp) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        let rel = entry.path()
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");

        if meta.is_file() {
            fp.insert(rel, (true, meta.len(), meta.modified()?.duration_since(UNIX_EPOCH).unwrap().as_secs()));
        } else if meta.is_dir() {
            fp.insert(rel.clone(), (false, 0, 0));
            walk_dir(root, &entry.path(), fp)?;
        }
    }
    Ok(())
}

pub fn write_dirfp<W: Write>(w: &mut W, map: &DirFp) -> io::Result<()> {
    w.write_all(&(map.len() as u64).to_le_bytes())?;

    for (key, &(flag, a, b)) in map {
        let key_bytes = key.as_bytes();
        w.write_all(&(key_bytes.len() as u32).to_le_bytes())?;
        w.write_all(key_bytes)?;
        w.write_all(&[flag as u8])?;
        w.write_all(&a.to_le_bytes())?;
        w.write_all(&b.to_le_bytes())?;
    }
    Ok(())
}

pub fn read_dirfp<R: Read>(r: &mut R) -> io::Result<DirFp> {
    let mut u64_buf = [0u8; 8];
    let mut u32_buf = [0u8; 4];
    let mut byte_buf = [0u8; 1];

    r.read_exact(&mut u64_buf)?;
    let count = u64::from_le_bytes(u64_buf) as usize;

    let mut map = HashMap::with_capacity(count);
    for _ in 0..count {
        r.read_exact(&mut u32_buf)?;
        let key_len = u32::from_le_bytes(u32_buf) as usize;

        let mut key_bytes = vec![0u8; key_len];
        r.read_exact(&mut key_bytes)?;
        let key = String::from_utf8(key_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        r.read_exact(&mut byte_buf)?;
        let flag = byte_buf[0] != 0;

        r.read_exact(&mut u64_buf)?;
        let a = u64::from_le_bytes(u64_buf);
        r.read_exact(&mut u64_buf)?;
        let b = u64::from_le_bytes(u64_buf);

        map.insert(key, (flag, a, b));
    }
    Ok(map)
}

/*
Opcodes:
 0 => Upload
 1 => Delete
 2 => Create Dir
*/
pub type DiffMap = HashMap<u8, Vec<String>>;

pub fn read_diffmap<R: Read>(r: &mut R) -> io::Result<DiffMap> {
    let mut u64_buf = [0u8; 8];
    let mut u32_buf = [0u8; 4];
    let mut byte_buf = [0u8; 1];

    r.read_exact(&mut byte_buf)?;
    let opcode_count = byte_buf[0] as usize;

    let mut map = HashMap::with_capacity(opcode_count);
    for _ in 0..opcode_count {
        r.read_exact(&mut byte_buf)?;
        let opcode = byte_buf[0];

        r.read_exact(&mut u64_buf)?;
        let path_count = u64::from_le_bytes(u64_buf) as usize;

        let mut paths = Vec::with_capacity(path_count);
        for _ in 0..path_count {
            r.read_exact(&mut u32_buf)?;
            let path_len = u32::from_le_bytes(u32_buf) as usize;

            let mut path_bytes = vec![0u8; path_len];
            r.read_exact(&mut path_bytes)?;
            let path = String::from_utf8(path_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            paths.push(path);
        }
        map.insert(opcode, paths);
    }
    Ok(map)
}
