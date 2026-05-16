use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::fs;

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
pub enum ArchiveFormat {
    Zip,
    TarGz,
    SevenZ,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AppConfig {
    pub format: ArchiveFormat,
    pub compression_level: u8,
    pub last_source: Option<PathBuf>,
    pub last_dest: Option<PathBuf>,
    pub encrypt_by_default: bool,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            format: ArchiveFormat::Zip,
            compression_level: 6,
            last_source: None,
            last_dest: None,
            encrypt_by_default: false,
        }
    }
}

impl AppConfig {
    pub fn load() -> Self {
        let path = dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("rust_archiver.json");
        if path.exists() {
            if let Ok(data) = fs::read_to_string(&path) {
                if let Ok(cfg) = serde_json::from_str(&data) { return cfg; }
            }
        }
        Self::default()
    }

    pub fn save(&self) {
        let path = dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("rust_archiver.json");
        if let Some(parent) = path.parent() { let _ = fs::create_dir_all(parent); }
        if let Ok(data) = serde_json::to_string_pretty(self) { let _ = fs::write(path, data); }
    }
}