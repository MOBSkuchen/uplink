use console::style;
use std::fs::{self, File};
use std::io::{self, Cursor, Read, Seek, Write};
use std::net::{SocketAddr, SocketAddrV4, TcpStream};
use std::path::Path;

use crate::error::{Error, Result};
use crate::output::{info, progress, spinner, step, RandomMessage};
use crate::protocol::{create_fingerprint, read_diffmap, write_dirfp, DiffMap, DirFp, OP_DOWNLOAD, OP_UPLOAD};
use crate::fasthash::{
    chunk_len, compare_hashes, hash_file, read_hashes, read_needed, read_part_path, reconstruct_file,
    write_hashes, write_needed, write_part_path, CHUNK_SIZE, PART_THRESHOLD,
};
use chrono::{DateTime, Utc};

const ZSTD_LEVEL: i32 = 3;

fn format_size(bytes: u64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    if bytes == 0 {
        return "0 B".to_string();
    }
    let i = (bytes as f64).log(1024.0).floor() as usize;
    let i = i.min(units.len() - 1);
    let value = bytes as f64 / 1024.0_f64.powi(i as i32);
    if i == 0 {
        format!("{} {}", bytes, units[i])
    } else {
        format!("{:.2} {}", value, units[i])
    }
}

fn write_name(stream: &mut TcpStream, name: &str) -> Result<()> {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err(Error::InvalidName);
    }
    if bytes.len() > u32::MAX as usize {
        return Err(Error::NameTooLong);
    }
    stream.write_all(&(bytes.len() as u32).to_le_bytes())?;
    stream.write_all(bytes)?;
    Ok(())
}

fn split_part_candidates(
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

fn pack_paths(paths: &[String], dir: &Path) -> Result<Vec<u8>> {
    if !dir.is_dir() {
        return Err(Error::NotADirectory(dir.to_path_buf()));
    }

    let total = paths
        .iter()
        .filter_map(|p| fs::metadata(dir.join(p)).ok())
        .map(|m| m.len())
        .sum();

    let pb = progress(total, RandomMessage::Packing)?;

    let mut tar_buf = Vec::new();
    {
        let writer = pb.wrap_write(&mut tar_buf);
        let mut builder = tar::Builder::new(writer);
        for path in paths {
            let full_path = dir.join(path);
            builder.append_path_with_name(&full_path, path)?;
        }
        builder.finish()?;
    }

    pb.finish_and_clear();

    let pb = progress(tar_buf.len() as u64, RandomMessage::Compressing)?;
    let mut compressed = Vec::new();
    {
        let mut encoder = zstd::Encoder::new(&mut compressed, ZSTD_LEVEL)?;
        let mut reader = pb.wrap_read(Cursor::new(&tar_buf));
        io::copy(&mut reader, &mut encoder)?;
        encoder.finish()?;
    }
    pb.finish_and_clear();

    let ratio = (compressed.len() as f64 / tar_buf.len() as f64) * 100f64 - 100f64;
    info(
        "compressed",
        format!(
            "{} {}",
            style(format_size(compressed.len() as u64)).green().bold(),
            style(format!("({:+.0}%)", ratio)).yellow()
        ),
    );
    Ok(compressed)
}

fn apply_diff(dest: &Path, dm: &DiffMap, data: &[u8], no_delete: bool) -> Result<()> {
    fs::create_dir_all(dest)?;

    if let Some(dirs) = dm.get(&2) {
        for dir in dirs {
            fs::create_dir_all(dest.join(dir))?;
        }
    }

    if dm.get(&0).is_some_and(|v| !v.is_empty()) && !data.is_empty() {
        let mut decompressed = Vec::new();
        {
            let pb = progress(data.len() as u64, RandomMessage::Decompressing)?;
            let mut decoder = zstd::Decoder::new(pb.wrap_read(data))?;
            io::copy(&mut decoder, &mut decompressed)?;
            pb.finish_and_clear();
        }

        let pb = progress(decompressed.len() as u64, RandomMessage::Unpacking)?;
        {
            let reader = pb.wrap_read(Cursor::new(&decompressed));
            let mut archive = tar::Archive::new(reader);
            archive.unpack(dest)?;
        }
        pb.finish_and_clear();
    }

    if !no_delete && let Some(deletions) = dm.get(&1) {
        for path in deletions {
            let full_path = dest.join(path);
            if full_path.is_dir() {
                fs::remove_dir_all(&full_path)?;
            } else if full_path.exists() {
                fs::remove_file(&full_path)?;
            }
        }
    }

    Ok(())
}

fn send_blob(stream: &mut TcpStream, data: &[u8], server: SocketAddrV4) -> Result<()> {
    stream.write_all(&(data.len() as u64).to_le_bytes())?;
    step(format!("Uploading to {}", style(server).cyan()));
    let pb = progress(data.len() as u64, RandomMessage::Uploading)?;
    let mut written = 0usize;
    let chunk = 64 * 1024;
    while written < data.len() {
        let end = (written + chunk).min(data.len());
        stream.write_all(&data[written..end])?;
        written = end;
        pb.set_position(written as u64);
    }
    pb.finish_and_clear();
    Ok(())
}

fn send_part_transfer(
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

        info(
            "part-sync",
            format!(
                "{} ({}/{} chunks)",
                style(path).cyan(),
                style(needed.len()).yellow().bold(),
                style(hashes.len()).dim(),
            ),
        );

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

fn recv_part_transfer(
    stream: &mut TcpStream,
    dest: &Path,
    spinner_pb: &indicatif::ProgressBar,
) -> Result<()> {
    let mut u32_buf = [0u8; 4];
    stream.read_exact(&mut u32_buf)?;
    let part_count = u32::from_le_bytes(u32_buf) as usize;

    for _ in 0..part_count {
        let path = read_part_path(stream)?;
        let (new_file_len, source_hashes) = read_hashes(stream)?;

        let local_path = dest.join(&path);
        let local_hashes = if local_path.exists() {
            hash_file(&local_path)?.1
        } else {
            Vec::new()
        };

        let needed = compare_hashes(&source_hashes, &local_hashes);

        spinner_pb.println(format!("  {} {}", style("part-sync:").dim(), format!(
            "{} ({}/{} chunks)",
            style(&path).cyan(),
            style(needed.len()).yellow().bold(),
            style(source_hashes.len()).dim(),
        )));

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
            fs::create_dir_all(parent)?;
        }
        fs::write(&local_path, &reconstructed)?;
    }
    Ok(())
}

pub struct Metadata {
    pub time: u64,
}

impl std::fmt::Display for Metadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let datetime: DateTime<Utc> = DateTime::from_timestamp(self.time as i64, 0).unwrap();
        write!(f, "{}", datetime.format("%d.%m.%Y %H:%M:%S"))
    }
}

fn read_meta(stream: &mut TcpStream) -> Result<Metadata> {
    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf)?;
    let time = u64::from_le_bytes(len_buf);
    let meta = Metadata { time };
    Ok(meta)
}

pub fn upload(server: SocketAddrV4, name: &str, dir: &Path) -> Result<()> {
    let mut stream = TcpStream::connect(SocketAddr::V4(server)).map_err(|source| Error::Connect {
        addr: server.to_string(),
        source,
    })?;
    stream.write_all(&[OP_UPLOAD])?;
    write_name(&mut stream, name)?;
    let fp = create_fingerprint(dir)?;
    write_dirfp(&mut stream, &fp)?;

    let sp = spinner(RandomMessage::WaitingOnServer)?;
    let dm = read_diffmap(&mut stream)?;

    let server_fp = crate::protocol::read_dirfp(&mut stream)?;

    if dm.is_empty() {
        sp.finish_and_clear();
        step("Already up to date");
        let mut ack = [0u8; 1];
        stream.read_exact(&mut ack)?;
        return Ok(());
    }

    let (part_paths, remaining_paths) = split_part_candidates(&dm, &fp, &server_fp);

    sp.finish_and_clear();
    if !part_paths.is_empty() {
        step(format!("Part-syncing {} large file(s)", style(part_paths.len()).cyan().bold()));
        send_part_transfer(&mut stream, dir, &part_paths)?;
    } else {
        stream.write_all(&0u32.to_le_bytes())?;
    }

    if !remaining_paths.is_empty() {
        step(format!("Packing {}", style(dir.display()).cyan()));
        let data = pack_paths(&remaining_paths, dir)?;
        send_blob(&mut stream, &data, server)?;
    } else {
        stream.write_all(&0u64.to_le_bytes())?;
    }

    let sp = spinner(RandomMessage::ServerProcessing)?;
    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack)?;
    sp.finish_and_clear();
    if ack[0] != 1 {
        return Err(Error::ServerRejected { opcode: OP_UPLOAD });
    }
    info(
        "uploaded",
        format!("{}", style(name).bold().yellow())
    );
    Ok(())
}

pub fn download(server: SocketAddrV4, name: &str, dest: &Path, no_delete: bool) -> Result<()> {
    step(format!(
        "Requesting '{}' from {}",
        style(name).cyan(),
        style(server).cyan()
    ));
    let mut stream = TcpStream::connect(SocketAddr::V4(server)).map_err(|source| Error::Connect {
        addr: server.to_string(),
        source,
    })?;
    stream.write_all(&[OP_DOWNLOAD])?;
    write_name(&mut stream, name)?;

    let local_fp = if dest.is_dir() {
        create_fingerprint(dest)?
    } else {
        DirFp::new()
    };
    write_dirfp(&mut stream, &local_fp)?;

    let sp = spinner(RandomMessage::WaitingOnServer)?;
    let mut status = [0u8; 1];
    stream.read_exact(&mut status)?;
    if status[0] == 0 {
        sp.finish_and_clear();
        return Err(Error::NotFound(name.to_string()));
    }

    let dm = read_diffmap(&mut stream)?;
    sp.finish_and_clear();

    let meta = read_meta(&mut stream)?;
    info("updated", style(meta).green().bold().to_string());
    let spinner_pb = spinner(RandomMessage::ClientProcessing)?;

    if dm.is_empty() {
        spinner_pb.finish_and_clear();
        step("Already up to date");
        return Ok(());
    }

    recv_part_transfer(&mut stream, dest, &spinner_pb)?;

    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf)?;
    let len = u64::from_le_bytes(len_buf);

    spinner_pb.finish_and_clear();
    if len > 0 {
        info("patch size", style(format_size(len)).green().bold().to_string());

        let pb = progress(len, RandomMessage::Downloading)?;
        let mut data = Vec::with_capacity(len as usize);
        {
            let mut reader = pb.wrap_read(stream.take(len));
            reader.read_to_end(&mut data)?;
        }
        pb.finish_and_clear();

        if (data.len() as u64) != len {
            return Err(Error::ShortRead {
                expected: len,
                got: data.len() as u64,
            });
        }

        step(format!("Unpacking into {}", style(dest.display()).cyan()));
        apply_diff(dest, &dm, &data, no_delete)?;
    } else {
        let has_deletes = dm.get(&1).is_some_and(|v| !v.is_empty());
        let has_mkdirs = dm.get(&2).is_some_and(|v| !v.is_empty());
        if has_deletes || has_mkdirs {
            step(format!("Applying changes to {}", style(dest.display()).cyan()));
            apply_diff(dest, &dm, &[], no_delete)?;
        }
    }

    info(
        "downloaded",
        format!("{}", style(name).bold().yellow())
    );
    Ok(())
}
