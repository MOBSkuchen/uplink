use std::{fs, io};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::time::UNIX_EPOCH;

type DirFp = HashMap<String, (bool, u64, u64)>;

pub fn create_fingerprint() -> io::Result<DirFp> {
    let entries = fs::read_dir("./")?;
    let mut fp = DirFp::new();
    for entry in entries {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_file() || meta.is_dir() {
            fp.insert(entry.file_name().into_string().unwrap(),
                      (meta.is_file(), meta.len(), meta.modified()?.duration_since(UNIX_EPOCH).unwrap().as_secs()));
        }
    }
    Ok(fp)
}

pub fn write_dirfp<W: Write>(w: &mut W, map: &DirFp) -> io::Result<()> {
    // Entry count as u64 LE
    w.write_all(&(map.len() as u64).to_le_bytes())?;

    for (key, &(flag, a, b)) in map {
        let key_bytes = key.as_bytes();
        // Key length as u32 LE, then key bytes
        w.write_all(&(key_bytes.len() as u32).to_le_bytes())?;
        w.write_all(key_bytes)?;
        // Flag as single byte, then two u64s LE
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