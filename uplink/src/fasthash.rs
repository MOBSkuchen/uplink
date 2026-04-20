use std::fs::File;
use std::io;
use std::io::{Read, Write};
use std::path::Path;

pub const CHUNK_SIZE: usize = 100 * 1024;
pub const PART_THRESHOLD: u64 = 1024 * 1024;

pub fn fast_hash64(data: &[u8]) -> u64 {
    const PRIME: u64 = 0x00000100000001B3;
    let mut h0: u64 = 0xcbf29ce484222325;
    let mut h1: u64 = 0xd2a98b26aa2b1e4f;
    let mut h2: u64 = 0x7bc4b4e19f3b6a2d;
    let mut h3: u64 = 0xf2350a13c5b8e417;

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        h0 ^= chunk[0] as u64; h0 = h0.wrapping_mul(PRIME);
        h1 ^= chunk[1] as u64; h1 = h1.wrapping_mul(PRIME);
        h2 ^= chunk[2] as u64; h2 = h2.wrapping_mul(PRIME);
        h3 ^= chunk[3] as u64; h3 = h3.wrapping_mul(PRIME);
    }

    for &byte in remainder {
        h0 ^= byte as u64; h0 = h0.wrapping_mul(PRIME);
    }

    h0 ^= h1.wrapping_mul(PRIME);
    h0 ^= h2.wrapping_mul(PRIME);
    h0 ^= h3.wrapping_mul(PRIME);
    h0.wrapping_mul(PRIME)
}

pub fn full_hash<R: Read>(mut f: R, len: usize) -> io::Result<Vec<u64>> {
    let num_chunks = len.div_ceil(CHUNK_SIZE);
    let mut hashes = Vec::with_capacity(num_chunks);
    let mut chunk = [0u8; CHUNK_SIZE];

    loop {
        let bytes_read = f.read(&mut chunk)?;
        if bytes_read == 0 { break; }
        hashes.push(fast_hash64(&chunk[..bytes_read]));
        if bytes_read < chunk.len() { break; }
    }

    Ok(hashes)
}

pub fn hash_file(path: &Path) -> io::Result<(u64, Vec<u64>)> {
    let meta = std::fs::metadata(path)?;
    let len = meta.len();
    let file = File::open(path)?;
    let hashes = full_hash(file, len as usize)?;
    Ok((len, hashes))
}

/// Compare source hashes against destination hashes.
/// Returns indices of chunks that differ or are new (source has more chunks).
pub fn compare_hashes(source: &[u64], dest: &[u64]) -> Vec<u32> {
    let mut needed = Vec::new();
    for i in 0..source.len() {
        if i >= dest.len() || source[i] != dest[i] {
            needed.push(i as u32);
        }
    }
    needed
}

/// Compute the byte size of a chunk given its index and the total file length.
pub fn chunk_len(idx: u32, file_len: u64) -> usize {
    let start = idx as u64 * CHUNK_SIZE as u64;
    let remaining = file_len.saturating_sub(start);
    remaining.min(CHUNK_SIZE as u64) as usize
}

// --- Wire protocol helpers ---

pub fn write_part_path<W: Write>(w: &mut W, path: &str) -> io::Result<()> {
    let bytes = path.as_bytes();
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(bytes)?;
    Ok(())
}

pub fn read_part_path<R: Read>(r: &mut R) -> io::Result<String> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    let len = u32::from_le_bytes(buf) as usize;
    let mut path_bytes = vec![0u8; len];
    r.read_exact(&mut path_bytes)?;
    String::from_utf8(path_bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub fn write_hashes<W: Write>(w: &mut W, file_len: u64, hashes: &[u64]) -> io::Result<()> {
    w.write_all(&file_len.to_le_bytes())?;
    w.write_all(&(hashes.len() as u32).to_le_bytes())?;
    for h in hashes {
        w.write_all(&h.to_le_bytes())?;
    }
    Ok(())
}

pub fn read_hashes<R: Read>(r: &mut R) -> io::Result<(u64, Vec<u64>)> {
    let mut u64_buf = [0u8; 8];
    let mut u32_buf = [0u8; 4];

    r.read_exact(&mut u64_buf)?;
    let file_len = u64::from_le_bytes(u64_buf);

    r.read_exact(&mut u32_buf)?;
    let count = u32::from_le_bytes(u32_buf) as usize;

    let mut hashes = Vec::with_capacity(count);
    for _ in 0..count {
        r.read_exact(&mut u64_buf)?;
        hashes.push(u64::from_le_bytes(u64_buf));
    }
    Ok((file_len, hashes))
}

pub fn write_needed<W: Write>(w: &mut W, indices: &[u32]) -> io::Result<()> {
    w.write_all(&(indices.len() as u32).to_le_bytes())?;
    for idx in indices {
        w.write_all(&idx.to_le_bytes())?;
    }
    Ok(())
}

pub fn read_needed<R: Read>(r: &mut R) -> io::Result<Vec<u32>> {
    let mut u32_buf = [0u8; 4];
    r.read_exact(&mut u32_buf)?;
    let count = u32::from_le_bytes(u32_buf) as usize;

    let mut indices = Vec::with_capacity(count);
    for _ in 0..count {
        r.read_exact(&mut u32_buf)?;
        indices.push(u32::from_le_bytes(u32_buf));
    }
    Ok(indices)
}

/// Reconstruct a file from old data + new chunks.
/// `old_path` is the destination's existing copy (may not exist or be shorter/longer).
/// Returns the complete new file contents.
pub fn reconstruct_file(
    old_path: &Path,
    new_file_len: u64,
    needed_indices: &[u32],
    chunk_data: &[Vec<u8>],
) -> io::Result<Vec<u8>> {
    let old_data = if old_path.exists() {
        std::fs::read(old_path)?
    } else {
        Vec::new()
    };

    let num_chunks = (new_file_len as usize).div_ceil(CHUNK_SIZE);
    let mut result = Vec::with_capacity(new_file_len as usize);
    let mut needed_iter = needed_indices.iter().zip(chunk_data.iter());
    let mut next_needed = needed_iter.next();

    for i in 0..num_chunks {
        let clen = chunk_len(i as u32, new_file_len);

        if let Some((&idx, data)) = next_needed {
            if idx == i as u32 {
                result.extend_from_slice(data);
                next_needed = needed_iter.next();
                continue;
            }
        }

        // Use old data for this chunk
        let start = i * CHUNK_SIZE;
        let end = (start + clen).min(old_data.len());
        if start < old_data.len() {
            result.extend_from_slice(&old_data[start..end]);
            // Pad with zeros if old chunk was shorter than expected
            if end - start < clen {
                result.resize(result.len() + clen - (end - start), 0);
            }
        } else {
            // Old file didn't have this chunk at all — shouldn't happen for matched chunks
            result.resize(result.len() + clen, 0);
        }
    }

    Ok(result)
}
