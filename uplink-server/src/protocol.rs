use std::io;
use std::collections::HashMap;
use std::io::{Read, Write};

// Shared constants
pub const OP_UPLOAD: u8 = 0;
pub const OP_DOWNLOAD: u8 = 1;
pub const OP_REMOVE: u8 = 2;
pub const MAX_NAME_LEN: u32 = 512;
pub const MAX_PAYLOAD: u64 = 16 * 1024 * 1024 * 1024;

/*
                                    Fingerprint
                    is file, (size, modification date)
*/
pub type DirFp = HashMap<String, (bool, u64, u64)>;

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

pub fn diff(new: &DirFp, old: &DirFp) -> DiffMap {
    let mut diff_map: DiffMap = HashMap::new();

    for (path, (is_file, new_size, new_mtime)) in new {
        match old.get(path) {
            Some((_, old_size, old_mtime)) => {
                if *is_file && (new_size != old_size || new_mtime != old_mtime) {
                    diff_map.entry(0).or_default().push(path.clone());
                }
            }
            None => {
                if *is_file {
                    diff_map.entry(0).or_default().push(path.clone());
                } else {
                    diff_map.entry(2).or_default().push(path.clone());
                }
            }
        }
    }

    for (path, _) in old {
        if !new.contains_key(path) {
            diff_map.entry(1).or_default().push(path.clone());
        }
    }

    diff_map
}

pub fn write_diffmap<W: Write>(w: &mut W, map: &DiffMap) -> io::Result<()> {
    w.write_all(&(map.len() as u8).to_le_bytes())?;

    for (opcode, paths) in map {
        w.write_all(&[*opcode])?;
        w.write_all(&(paths.len() as u64).to_le_bytes())?;

        for path in paths {
            let path_bytes = path.as_bytes();
            w.write_all(&(path_bytes.len() as u32).to_le_bytes())?;
            w.write_all(path_bytes)?;
        }
    }
    Ok(())
}
