use std::fs::File;
use std::io::{self, Cursor, Read, Seek, Write};
use std::net::TcpStream;
use std::path::Path;
use crate::error::{Error, Result};
use crate::protocol::{DirFp, DiffMap};
use crate::fasthash::{
    hash_file, compare_hashes, chunk_len, reconstruct_file,
    write_part_path, write_hashes, read_hashes, write_needed, read_needed, read_part_path, CHUNK_SIZE, PART_THRESHOLD,
};

pub fn split_part_candidates(
    dm: &DiffMap,
    source_fp: &DirFp,
    other_fp: &DirFp,
) -> (Vec<String>, Vec<String>) {
    let mut part_paths = Vec::new();
    let mut remaining = Vec::new();

    if let Some(uploads) = dm.get(&0) {
        for path in uploads {
            let in_other = other_fp.contains_key(path);
            let source_size = source_fp.get(path).map(|(_, sz, _)| *sz).unwrap_or(0);
            if in_other && source_size >= PART_THRESHOLD {
                part_paths.push(path.clone());
            } else {
                remaining.push(path.clone());
            }
        }
    }

    part_paths.sort();
    remaining.sort();
    (part_paths, remaining)
}

pub fn recv_part_transfer_server(stream: &mut TcpStream, dir: &Path) -> Result<()> {
    let mut u32_buf = [0u8; 4];
    stream.read_exact(&mut u32_buf)?;
    let part_count = u32::from_le_bytes(u32_buf) as usize;

    for _ in 0..part_count {
        let path = read_part_path(stream)?;
        let (new_file_len, source_hashes) = read_hashes(stream)?;

        let local_path = dir.join(&path);
        let local_hashes = if local_path.exists() {
            hash_file(&local_path)?.1
        } else {
            Vec::new()
        };

        let needed = compare_hashes(&source_hashes, &local_hashes);
        println!("  part-sync '{}': {}/{} chunks needed", path, needed.len(), source_hashes.len());
        write_needed(stream, &needed)?;

        let mut chunk_data = Vec::with_capacity(needed.len());
        for &idx in &needed {
            let clen = chunk_len(idx, new_file_len);
            let mut buf = vec![0u8; clen];
            stream.read_exact(&mut buf)?;
            chunk_data.push(buf);
        }

        let reconstructed = reconstruct_file(&local_path, new_file_len, &needed, &chunk_data)?;
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&local_path, &reconstructed)?;
    }
    Ok(())
}

pub fn send_part_transfer_server(
    stream: &mut TcpStream,
    dir: &Path,
    part_paths: &[String],
) -> Result<()> {
    stream.write_all(&(part_paths.len() as u32).to_le_bytes())?;

    for path in part_paths {
        let full = dir.join(path);
        let (file_len, hashes) = hash_file(&full)?;

        write_part_path(stream, path)?;
        write_hashes(stream, file_len, &hashes)?;

        let needed = read_needed(stream)?;
        println!("  part-sync '{}': {}/{} chunks requested", path, needed.len(), hashes.len());

        for &idx in &needed {
            let clen = chunk_len(idx, file_len);
            let start = idx as u64 * CHUNK_SIZE as u64;
            let mut buf = vec![0u8; clen];
            let mut f = File::open(&full)?;
            f.seek(io::SeekFrom::Start(start))?;
            f.read_exact(&mut buf)?;
            stream.write_all(&buf)?;
        }
    }
    Ok(())
}

pub fn recv_blob(stream: &mut TcpStream, dir: &Path) -> Result<()> {
    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf)?;
    let len = u64::from_le_bytes(len_buf);
    if len == 0 { return Ok(()); }
    if len > crate::protocol::MAX_PAYLOAD {
        return Err(Error::PayloadTooLarge(len));
    }

    let mut compressed = vec![0u8; len as usize];
    let mut received = 0usize;
    while received < len as usize {
        let n = stream.read(&mut compressed[received..])?;
        if n == 0 {
            return Err(Error::ClientDisconnected);
        }
        received += n;
    }

    let mut decompressed = Vec::new();
    let mut decoder = zstd::Decoder::new(Cursor::new(&compressed))?;
    io::copy(&mut decoder, &mut decompressed)?;

    let mut archive = tar::Archive::new(Cursor::new(&decompressed));
    archive.unpack(dir)?;
    println!("  received {} bytes compressed", len);
    Ok(())
}
