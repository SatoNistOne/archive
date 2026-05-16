use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}, mpsc};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::time::UNIX_EPOCH;

use walkdir::WalkDir;
use zip::write::FileOptions;
use zip::{ZipWriter, ZipArchive, CompressionMethod, DateTime};
use zip::result::ZipError;
use flate2::{write::GzEncoder, read::GzDecoder, Compression};
use tar::Builder;
use sevenz_rust;
use thiserror::Error;
use age::{DecryptError, EncryptError, Encryptor, Decryptor, secrecy::Secret};
use tempfile::NamedTempFile;
use filetime::FileTime;
use time::OffsetDateTime;

#[derive(Error, Debug)]
pub enum ArchiverError {
    #[error("Защита от выхода за пределы директории: {0}")]
    PathTraversal(String),
    #[error("Слишком много файлов: {0}")]
    TooManyFiles(usize),
    #[error("Превышен максимальный размер: {0} байт")]
    SizeExceeded(u64),
    #[error("Отменено пользователем")]
    Cancelled,
    #[error("Ошибка ввода-вывода: {0}")]
    Io(#[from] std::io::Error),
    #[error("Ошибка архива: {0}")]
    Archive(String),
    #[error("Ошибка ZIP: {0}")]
    Zip(#[from] ZipError),
    #[error("Ошибка 7Z: {0}")]
    SevenZ(#[from] sevenz_rust::Error),
    #[error("Ошибка шифрования: {0}")]
    Crypto(String),
}

impl From<walkdir::Error> for ArchiverError {
    fn from(e: walkdir::Error) -> Self {
        ArchiverError::Io(e.into_io_error().unwrap_or_else(|| std::io::Error::other("ошибка walkdir")))
    }
}

impl From<std::path::StripPrefixError> for ArchiverError {
    fn from(e: std::path::StripPrefixError) -> Self {
        ArchiverError::Archive(e.to_string())
    }
}

impl From<DecryptError> for ArchiverError {
    fn from(e: DecryptError) -> Self {
        ArchiverError::Crypto(e.to_string())
    }
}

impl From<EncryptError> for ArchiverError {
    fn from(e: EncryptError) -> Self {
        ArchiverError::Crypto(e.to_string())
    }
}

pub const MAX_FILES: usize = 50_000;
pub const MAX_SIZE: u64 = 35 * 1024 * 1024 * 1024;
pub const BUF_SIZE: usize = 65_536;

#[derive(Debug)]
pub enum ArchiverMsg {
    Progress(f32, u64),
    Finished(Result<(), ArchiverError>),
    PreviewReady(Result<Vec<(String, u64, bool)>, ArchiverError>),
}

pub fn calc_size(paths: &[PathBuf], cancel: &Arc<AtomicBool>) -> Result<(u64, usize), ArchiverError> {
    let mut total_size = 0u64;
    let mut file_count = 0usize;
    for path in paths {
        if path.is_dir() {
            for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
                if cancel.load(Ordering::Relaxed) { return Err(ArchiverError::Cancelled); }
                if entry.file_type().is_file() {
                    total_size += entry.metadata()?.len();
                    file_count += 1;
                }
                if file_count > MAX_FILES { return Err(ArchiverError::TooManyFiles(file_count)); }
                if total_size > MAX_SIZE { return Err(ArchiverError::SizeExceeded(total_size)); }
            }
        } else if path.is_file() {
            total_size += path.metadata()?.len();
            file_count += 1;
            if file_count > MAX_FILES { return Err(ArchiverError::TooManyFiles(file_count)); }
            if total_size > MAX_SIZE { return Err(ArchiverError::SizeExceeded(total_size)); }
        }
    }
    Ok((total_size, file_count))
}

fn get_entry_name(src: &Path, base: &Path) -> Result<String, ArchiverError> {
    if let Ok(rel) = src.strip_prefix(base) {
        if let Some(s) = rel.to_str() { return Ok(s.replace("\\", "/")); }
    }
    if let Some(name) = src.file_name().and_then(|n| n.to_str()) {
        Ok(name.replace("\\", "/"))
    } else {
        Err(ArchiverError::Archive("Некорректный путь".into()))
    }
}

pub fn compress_zip(sources: &[PathBuf], destination: &PathBuf, level: u8, cancel: &Arc<AtomicBool>, tx: &mpsc::Sender<ArchiverMsg>) -> Result<(), ArchiverError> {
    let (total_size, _) = calc_size(sources, cancel)?;
    let mut zip_writer = ZipWriter::new(BufWriter::new(File::create(destination)?));
    let mut processed = 0u64;
    let mut buffer = [0u8; BUF_SIZE];
    
    for src in sources {
        if src.is_dir() {
            let base = src.parent().unwrap_or(src);
            for entry in WalkDir::new(src).into_iter().filter_map(|e| e.ok()) {
                if cancel.load(Ordering::Relaxed) { return Err(ArchiverError::Cancelled); }
                if entry.file_type().is_file() {
                    let meta = entry.metadata()?;
                    let mtime = meta.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                        .and_then(|d| OffsetDateTime::from_unix_timestamp(d.as_secs() as i64).ok())
                        .and_then(|dt| DateTime::from_date_and_time(dt.year() as u16, dt.month().into(), dt.day(), dt.hour(), dt.minute(), dt.second()).ok())
                        .unwrap_or_else(DateTime::default);
                    let options: FileOptions<'_, ()> = FileOptions::default().compression_method(CompressionMethod::Deflated).compression_level(Some(level as i64)).last_modified_time(mtime);
                    let entry_name = get_entry_name(entry.path(), base)?;
                    zip_writer.start_file(&entry_name, options)?;
                    let mut file_reader = BufReader::new(File::open(entry.path())?);
                    loop { let bytes_read = file_reader.read(&mut buffer)?; if bytes_read == 0 { break; } zip_writer.write_all(&buffer[..bytes_read])?; processed += bytes_read as u64; tx.send(ArchiverMsg::Progress((processed as f32 / total_size as f32).min(1.0), processed)).ok(); }
                }
            }
        } else if src.is_file() {
            let base = src.parent().unwrap_or(src);
            let meta = src.metadata()?;
            let mtime = meta.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .and_then(|d| OffsetDateTime::from_unix_timestamp(d.as_secs() as i64).ok())
                .and_then(|dt| DateTime::from_date_and_time(dt.year() as u16, dt.month().into(), dt.day(), dt.hour(), dt.minute(), dt.second()).ok())
                .unwrap_or_else(DateTime::default);
            let options: FileOptions<'_, ()> = FileOptions::default().compression_method(CompressionMethod::Deflated).compression_level(Some(level as i64)).last_modified_time(mtime);
            let entry_name = get_entry_name(src, base)?;
            zip_writer.start_file(&entry_name, options)?;
            let mut file_reader = BufReader::new(File::open(src)?);
            loop { let bytes_read = file_reader.read(&mut buffer)?; if bytes_read == 0 { break; } zip_writer.write_all(&buffer[..bytes_read])?; processed += bytes_read as u64; tx.send(ArchiverMsg::Progress((processed as f32 / total_size as f32).min(1.0), processed)).ok(); }
        }
    }
    zip_writer.finish()?;
    Ok(())
}

pub fn compress_tgz(sources: &[PathBuf], destination: &PathBuf, level: u8, cancel: &Arc<AtomicBool>, tx: &mpsc::Sender<ArchiverMsg>) -> Result<(), ArchiverError> {
    let (total_size, _) = calc_size(sources, cancel)?;
    let encoder = GzEncoder::new(BufWriter::new(File::create(destination)?), Compression::new(level.into()));
    let mut tar_builder = Builder::new(encoder);
    let mut processed = 0u64;
    
    for src in sources {
        if src.is_dir() {
            let base = src.parent().unwrap_or(src);
            for entry in WalkDir::new(src).into_iter().filter_map(|e| e.ok()) {
                if cancel.load(Ordering::Relaxed) { return Err(ArchiverError::Cancelled); }
                if entry.file_type().is_file() {
                    let meta = entry.metadata()?;
                    let mtime = meta.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map(|d| d.as_secs()).unwrap_or(0);
                    let entry_name = get_entry_name(entry.path(), base)?;
                    let mut file = File::open(entry.path())?;
                    let mut header = tar::Header::new_gnu(); header.set_path(&entry_name)?; header.set_size(meta.len()); header.set_mtime(mtime); header.set_mode(0o644); header.set_cksum();
                    tar_builder.append(&header, &mut file)?; processed += meta.len(); tx.send(ArchiverMsg::Progress((processed as f32 / total_size as f32).min(1.0), processed)).ok();
                }
            }
        } else if src.is_file() {
            let base = src.parent().unwrap_or(src);
            let meta = src.metadata()?;
            let mtime = meta.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map(|d| d.as_secs()).unwrap_or(0);
            let entry_name = get_entry_name(src, base)?;
            let mut file = File::open(src)?;
            let mut header = tar::Header::new_gnu(); header.set_path(&entry_name)?; header.set_size(meta.len()); header.set_mtime(mtime); header.set_mode(0o644); header.set_cksum();
            tar_builder.append(&header, &mut file)?; processed += meta.len(); tx.send(ArchiverMsg::Progress((processed as f32 / total_size as f32).min(1.0), processed)).ok();
        }
    }
    tar_builder.into_inner()?.finish()?;
    Ok(())
}

pub fn compress_7z(sources: &[PathBuf], destination: &PathBuf, cancel: &Arc<AtomicBool>, tx: &mpsc::Sender<ArchiverMsg>) -> Result<(), ArchiverError> {
    if sources.is_empty() { return Err(ArchiverError::Archive("Нет файлов для архивации".into())); }
    let (_, _) = calc_size(sources, cancel)?;
    let dest_file = File::create(destination)?;
    sevenz_rust::compress(sources.first().unwrap(), dest_file)?;
    tx.send(ArchiverMsg::Progress(1.0, 0)).ok();
    Ok(())
}

pub fn encrypt_archive(source: &PathBuf, destination: &PathBuf, password: &str, cancel: &Arc<AtomicBool>, tx: &mpsc::Sender<ArchiverMsg>) -> Result<(), ArchiverError> {
    let src_file = File::open(source)?;
    let meta = src_file.metadata()?;
    let total_size = meta.len();
    let mut reader = BufReader::new(src_file);
    let mut writer = BufWriter::new(File::create(destination)?);
    let secret = Secret::new(password.to_string());
    let encryptor = Encryptor::with_user_passphrase(secret);
    let mut wrapped = encryptor.wrap_output(&mut writer)?;
    let mut buffer = [0u8; BUF_SIZE];
    let mut processed = 0u64;
    loop {
        if cancel.load(Ordering::Relaxed) { return Err(ArchiverError::Cancelled); }
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 { break; }
        wrapped.write_all(&buffer[..bytes_read])?;
        processed += bytes_read as u64;
        tx.send(ArchiverMsg::Progress((processed as f32 / total_size as f32).min(1.0), processed)).ok();
    }
    wrapped.finish()?;
    std::fs::remove_file(source)?;
    Ok(())
}

pub fn decrypt_to_temp(source: &PathBuf, password: &str) -> Result<NamedTempFile, ArchiverError> {
    let src_file = File::open(source)?;
    let reader = BufReader::new(src_file);
    let secret = Secret::new(password.to_string());
    let decryptor = Decryptor::new(reader)?;
    let mut decrypted = match decryptor { age::Decryptor::Passphrase(p) => p.decrypt(&secret, None)?, _ => return Err(ArchiverError::Crypto("Неподдерживаемый тип шифрования".into())) };
    let mut dest_file = NamedTempFile::new()?;
    std::io::copy(&mut decrypted, &mut dest_file)?;
    Ok(dest_file)
}

pub fn extract_zip(archive: &PathBuf, destination: &PathBuf, cancel: &Arc<AtomicBool>, _tx: &mpsc::Sender<ArchiverMsg>) -> Result<(), ArchiverError> {
    let mut zip_archive = ZipArchive::new(BufReader::new(File::open(archive)?))?;
    if zip_archive.len() > MAX_FILES { return Err(ArchiverError::TooManyFiles(zip_archive.len())); }
    let mut buffer = [0u8; BUF_SIZE];
    let base_dest = std::fs::canonicalize(destination).unwrap_or(destination.clone());
    for index in 0..zip_archive.len() {
        if cancel.load(Ordering::Relaxed) { return Err(ArchiverError::Cancelled); }
        let mut file = zip_archive.by_index(index)?;
        let output_path = match file.enclosed_name() { Some(p) => destination.join(p), None => continue };
        if let Ok(canonical) = output_path.canonicalize() { if !canonical.starts_with(&base_dest) { return Err(ArchiverError::PathTraversal(output_path.display().to_string())); } }
        if file.name().ends_with('/') { std::fs::create_dir_all(&output_path)?; } else {
            if let Some(parent) = output_path.parent() { std::fs::create_dir_all(parent)?; }
            let mut output_file = File::create(&output_path)?;
            loop { let bytes_read = file.read(&mut buffer)?; if bytes_read == 0 { break; } output_file.write_all(&buffer[..bytes_read])?; }
            if let Some(mtime) = file.last_modified() { if let Ok(dt) = OffsetDateTime::try_from(mtime) { let _ = filetime::set_file_mtime(&output_path, FileTime::from_unix_time(dt.unix_timestamp(), 0)); } }
        }
    }
    Ok(())
}

pub fn extract_tgz(archive: &PathBuf, destination: &PathBuf, cancel: &Arc<AtomicBool>, _tx: &mpsc::Sender<ArchiverMsg>) -> Result<(), ArchiverError> {
    let mut tar_archive = tar::Archive::new(GzDecoder::new(BufReader::new(File::open(archive)?)));
    let base_dest = std::fs::canonicalize(destination).unwrap_or(destination.clone());
    for entry_result in tar_archive.entries()? {
        if cancel.load(Ordering::Relaxed) { return Err(ArchiverError::Cancelled); }
        let mut entry = entry_result?;
        let output_path = destination.join(entry.path()?.to_path_buf());
        if let Ok(canonical) = output_path.canonicalize() { if !canonical.starts_with(&base_dest) { return Err(ArchiverError::PathTraversal(output_path.display().to_string())); } }
        entry.unpack(&output_path)?;
        let mtime = entry.header().mtime().unwrap_or(0) as i64;
        let _ = filetime::set_file_mtime(&output_path, FileTime::from_unix_time(mtime, 0));
    }
    Ok(())
}

pub fn extract_7z(archive: &PathBuf, destination: &PathBuf, _cancel: &Arc<AtomicBool>, tx: &mpsc::Sender<ArchiverMsg>) -> Result<(), ArchiverError> {
    sevenz_rust::decompress_file(archive, destination)?;
    tx.send(ArchiverMsg::Progress(1.0, 0)).ok();
    Ok(())
}

pub fn preview_zip(source: &PathBuf) -> Result<Vec<(String, u64, bool)>, ArchiverError> {
    let mut zip_archive = ZipArchive::new(BufReader::new(File::open(source)?))?;
    let mut entries = Vec::new();
    for i in 0..zip_archive.len().min(1000) { if let Ok(file) = zip_archive.by_index(i) { entries.push((file.name().to_string(), file.size(), file.is_dir())); } }
    Ok(entries)
}

pub fn preview_tgz(source: &PathBuf) -> Result<Vec<(String, u64, bool)>, ArchiverError> {
    let mut tar_archive = tar::Archive::new(GzDecoder::new(BufReader::new(File::open(source)?)));
    let mut entries = Vec::new();
    let mut count = 0;
    for entry_result in tar_archive.entries()? {
        if count >= 1000 { break; }
        if let Ok(entry) = entry_result { if let Ok(path) = entry.path() { entries.push((path.to_string_lossy().to_string(), entry.size(), entry.header().entry_type().is_dir())); count += 1; } }
    }
    Ok(entries)
}

pub fn preview_7z(_source: &PathBuf) -> Result<Vec<(String, u64, bool)>, ArchiverError> { Ok(Vec::new()) }