use std::path::PathBuf;
use std::sync::{Arc, atomic::AtomicBool, mpsc};
use std::thread;
use std::process::Command;
use std::time::Instant;

use eframe::egui;
use egui::Widget;
use rfd::FileDialog;

use crate::config::{AppConfig, ArchiveFormat};
use crate::archiver::{
    ArchiverError, ArchiverMsg, calc_size,
    compress_zip, extract_zip, preview_zip,
    compress_tgz, extract_tgz, preview_tgz,
    compress_7z, extract_7z, preview_7z,
    encrypt_archive, decrypt_to_temp
};

const MAX_SOURCE_FILES: usize = 50;
const MAX_SOURCE_FOLDERS: usize = 5;

pub struct ArchiverApp {
    sources: Vec<PathBuf>,
    destination: Option<PathBuf>,
    format: ArchiveFormat,
    level: u8,
    password: String,
    show_password: bool,
    encrypt: bool,
    manual_mode: Option<bool>,
    progress: f32,
    status: String,
    eta: String,
    is_busy: bool,
    is_success: bool,
    config: AppConfig,
    cancel: Arc<AtomicBool>,
    tx: Option<mpsc::Sender<ArchiverMsg>>,
    rx: Option<mpsc::Receiver<ArchiverMsg>>,
    worker: Option<thread::JoinHandle<()>>,
    show_preview: bool,
    preview_data: Vec<(String, u64, bool)>,
    preview_loading: bool,
    start_time: Option<Instant>,
    total_bytes: u64,
}

impl ArchiverApp {
    pub fn new(cc: &eframe::CreationContext<'_>, initial_path: Option<PathBuf>) -> Self {
        cc.egui_ctx.set_visuals(egui::Visuals::dark());
        let config = AppConfig::load();
        let mut sources = Vec::new();
        if let Some(p) = initial_path { sources.push(p); } else if let Some(p) = config.last_source.clone() { sources.push(p); }
        Self {
            sources,
            destination: config.last_dest.clone(),
            format: config.format,
            level: config.compression_level,
            password: String::new(),
            show_password: false,
            encrypt: config.encrypt_by_default,
            manual_mode: None,
            progress: 0.0,
            status: "Готов".into(),
            eta: "00:00:00".into(),
            is_busy: false,
            is_success: false,
            config,
            cancel: Arc::new(AtomicBool::new(false)),
            tx: None,
            rx: None,
            worker: None,
            show_preview: false,
            preview_data: Vec::new(),
            preview_loading: false,
            start_time: None,
            total_bytes: 0,
        }
    }

    fn count_limits(&self) -> (usize, usize) {
        let files = self.sources.iter().filter(|p| p.is_file()).count();
        let folders = self.sources.iter().filter(|p| p.is_dir()).count();
        (files, folders)
    }

    fn can_add(&self, is_dir: bool) -> bool {
        let (f, d) = self.count_limits();
        if is_dir { d < MAX_SOURCE_FOLDERS } else { f < MAX_SOURCE_FILES }
    }

    fn is_compress_mode(&self) -> bool {
        if let Some(force) = self.manual_mode { return force; }
        if self.sources.is_empty() { return true; }
        if self.sources.len() > 1 { return true; }
        if let Some(src) = self.sources.first() {
            if src.is_dir() { return true; }
            match self.format {
                ArchiveFormat::Zip => !src.extension().map_or(false, |e| e == "zip"),
                ArchiveFormat::TarGz => !src.extension().map_or(false, |e| e == "gz" || e == "tgz"),
                ArchiveFormat::SevenZ => !src.extension().map_or(false, |e| e == "7z"),
            }
        } else { true }
    }

    fn validate_extract_format(&self) -> Result<(), String> {
        if !self.is_compress_mode() {
            if let Some(src) = self.sources.first() {
                if let Some(ext) = src.extension().and_then(|e| e.to_str()) {
                    let expected = match self.format { ArchiveFormat::Zip => "zip", ArchiveFormat::TarGz => "tar.gz", ArchiveFormat::SevenZ => "7z" };
                    if ext != "enc" && ext != expected { return Err(format!("Файл '.{}' не является архивом формата {:?}", ext, self.format)); }
                }
            }
        }
        Ok(())
    }

    fn update_destination_name(&mut self) {
        if self.sources.is_empty() { return; }
        let ext = match self.format { ArchiveFormat::Zip => "zip", ArchiveFormat::TarGz => "tar.gz", ArchiveFormat::SevenZ => "7z" };
        if self.is_compress_mode() {
            let parent = self.sources.first().and_then(|s| s.parent()).unwrap_or(&self.sources[0]);
            let stem = if self.sources.len() == 1 { self.sources[0].file_stem().and_then(|s| s.to_str()).unwrap_or("архив") } else { "архив" };
            self.destination = Some(parent.join(format!("{}-архив.{}", stem, ext)));
        } else if let Some(parent) = self.sources.first().and_then(|s| s.parent()) {
            self.destination = Some(parent.to_path_buf());
        }
    }

    fn format_eta(secs: f64) -> String {
        if secs.is_nan() || secs.is_infinite() || secs < 0.0 { return "00:00:00".into(); }
        let h = secs / 3600.0; let m = (secs % 3600.0) / 60.0; let s = secs % 60.0;
        format!("{:02}:{:02}:{:02}", h as u64, m as u64, s as u64)
    }

    fn run_task(&mut self) {
        let (f, d) = self.count_limits();
        if f == 0 && d == 0 { self.status = "Выберите файлы или папки".into(); return; }
        if self.destination.is_none() { self.status = "Укажите путь назначения".into(); return; }
        if let Err(e) = self.validate_extract_format() { self.status = format!("Ошибка: {}", e); return; }
        if self.is_compress_mode() {
            if let Some(dest) = &self.destination { if dest.exists() { self.status = "Внимание: файл назначения существует и будет перезаписан.".into(); } }
        }
        self.is_success = false; self.is_busy = true; self.progress = 0.0; self.eta = "00:00:00".into();
        self.cancel.store(false, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(); self.tx = Some(tx.clone()); self.rx = Some(rx);
        let sources = self.sources.clone(); let destination = self.destination.clone().unwrap();
        let format = self.format; let level = self.level;
        let password = if self.encrypt { self.password.clone() } else { String::new() };
        let cancel = self.cancel.clone(); let is_compress = self.is_compress_mode(); let encrypt = self.encrypt;
        let (total_size, _) = calc_size(&sources, &cancel).unwrap_or((0, 0));
        self.total_bytes = total_size; self.start_time = Some(Instant::now());
        self.status = if is_compress { "Архивирование..." } else { "Распаковка..." }.into();

        self.worker = Some(thread::spawn(move || {
            let result: Result<(), ArchiverError> = (|| -> Result<(), ArchiverError> {
                if is_compress {
                    let temp_ext = match format { ArchiveFormat::Zip => "zip", ArchiveFormat::TarGz => "tar.gz", ArchiveFormat::SevenZ => "7z" };
                    let temp_dest = destination.with_extension(format!("tmp.{}", temp_ext));
                    let compress_res = match format {
                        ArchiveFormat::Zip => compress_zip(&sources, &temp_dest, level, &cancel, &tx),
                        ArchiveFormat::TarGz => compress_tgz(&sources, &temp_dest, level, &cancel, &tx),
                        ArchiveFormat::SevenZ => compress_7z(&sources, &temp_dest, &cancel, &tx),
                    };
                    compress_res?;
                    if encrypt && !password.is_empty() {
                        let enc_dest = destination.with_extension("enc");
                        encrypt_archive(&temp_dest, &enc_dest, &password, &cancel, &tx)?;
                    } else {
                        std::fs::rename(&temp_dest, &destination).map_err(ArchiverError::Io)?;
                    }
                } else {
                    let src = &sources[0];
                    let is_encrypted = src.extension().map_or(false, |ext| ext == "enc");
                    if is_encrypted {
                        if password.is_empty() {
                            return Err(ArchiverError::Crypto("Требуется пароль для расшифровки".into()));
                        }
                        let temp_file = decrypt_to_temp(src, &password)?;
                        match format {
                            ArchiveFormat::Zip => extract_zip(&temp_file.path().to_path_buf(), &destination, &cancel, &tx)?,
                            ArchiveFormat::TarGz => extract_tgz(&temp_file.path().to_path_buf(), &destination, &cancel, &tx)?,
                            ArchiveFormat::SevenZ => extract_7z(&temp_file.path().to_path_buf(), &destination, &cancel, &tx)?,
                        }
                    } else {
                        match format {
                            ArchiveFormat::Zip => extract_zip(src, &destination, &cancel, &tx)?,
                            ArchiveFormat::TarGz => extract_tgz(src, &destination, &cancel, &tx)?,
                            ArchiveFormat::SevenZ => extract_7z(src, &destination, &cancel, &tx)?,
                        }
                    }
                }
                Ok(())
            })();
            let _ = tx.send(ArchiverMsg::Finished(result));
        }));
    }

    fn cancel_task(&mut self) { self.cancel.store(true, std::sync::atomic::Ordering::Relaxed); self.status = "Отмена...".into(); }
    fn clear_selection(&mut self) { self.sources.clear(); self.destination = None; self.password.clear(); self.progress = 0.0; self.status = "Очищено".into(); self.eta = "00:00:00".into(); self.is_success = false; }
    fn open_destination(&self) { if let Some(dest) = &self.destination { let path = if dest.is_file() { dest.parent().unwrap_or(dest) } else { dest }; let _ = Command::new("explorer").arg(path).spawn(); } }

    fn run_preview(&mut self) {
        if self.sources.is_empty() { return; }
        self.preview_loading = true; self.show_preview = true; self.preview_data.clear();
        let source = self.sources.first().unwrap().clone(); let format = self.format;
        let password = if self.encrypt { self.password.clone() } else { String::new() };
        let (tx, rx) = mpsc::channel(); self.rx = Some(rx);
        thread::spawn(move || {
            let result: Result<Vec<(String, u64, bool)>, ArchiverError> = (|| -> Result<Vec<(String, u64, bool)>, ArchiverError> {
                if source.extension().map_or(false, |ext| ext == "enc") {
                    if password.is_empty() {
                        return Err(ArchiverError::Crypto("Требуется пароль".into()));
                    }
                    let temp_file = decrypt_to_temp(&source, &password)?;
                    match format {
                        ArchiveFormat::Zip => preview_zip(&temp_file.path().to_path_buf()),
                        ArchiveFormat::TarGz => preview_tgz(&temp_file.path().to_path_buf()),
                        ArchiveFormat::SevenZ => preview_7z(&temp_file.path().to_path_buf()),
                    }
                } else {
                    match format {
                        ArchiveFormat::Zip => preview_zip(&source),
                        ArchiveFormat::TarGz => preview_tgz(&source),
                        ArchiveFormat::SevenZ => preview_7z(&source),
                    }
                }
            })();
            let _ = tx.send(ArchiverMsg::PreviewReady(result));
        });
    }
}

impl eframe::App for ArchiverApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let dropped_files = ctx.input(|i| i.raw.dropped_files.clone());
        if !dropped_files.is_empty() && !self.is_busy {
            for f in dropped_files.iter() {
                if let Some(p) = &f.path {
                    if !self.can_add(p.is_dir()) {
                        self.status = format!("Достигнут лимит: файлы {}/{} или папки {}/{}", 
                            self.count_limits().0, MAX_SOURCE_FILES, self.count_limits().1, MAX_SOURCE_FOLDERS);
                        break;
                    }
                    if !self.sources.contains(p) { self.sources.push(p.clone()); }
                }
            }
            if !self.sources.is_empty() { self.update_destination_name(); }
        }
        if let Some(rx) = &self.rx {
            while let Ok(message) = rx.try_recv() {
                match message {
                    ArchiverMsg::Progress(value, processed) => {
                        self.progress = value;
                        if let Some(start) = self.start_time {
                            let elapsed = start.elapsed().as_secs_f64();
                            if processed > 0 && elapsed > 0.1 { let speed = processed as f64 / elapsed; let remaining = (self.total_bytes as f64 - processed as f64) / speed; self.eta = Self::format_eta(remaining); }
                        }
                    }
                    ArchiverMsg::Finished(result) => {
                        self.is_busy = false; self.is_success = result.is_ok();
                        self.status = match &result { Ok(_) => "Готово".into(), Err(e) => format!("Ошибка: {}", e) };
                        self.progress = if result.is_ok() { 1.0 } else { self.progress }; self.eta = "00:00:00".into(); self.config.save(); self.worker = None;
                    }
                    ArchiverMsg::PreviewReady(result) => { match result { Ok(items) => self.preview_data = items, Err(e) => self.status = format!("Ошибка: {}", e) } self.preview_loading = false; }
                }
            }
        }
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.style_mut().spacing.item_spacing = egui::vec2(8.0, 8.0);
            ui.heading("Archiver"); ui.separator();
            
            ui.horizontal(|ui| {
                ui.label("Режим:");
                if ui.selectable_label(self.manual_mode == Some(true), "Сжать").clicked() { self.manual_mode = Some(true); }
                if ui.selectable_label(self.manual_mode == Some(false), "Распаковать").clicked() { self.manual_mode = Some(false); }
                if ui.selectable_label(self.manual_mode.is_none(), "Авто").clicked() { self.manual_mode = None; }
            });
            
            let is_compress = self.is_compress_mode();
            ui.horizontal(|ui| { 
                ui.label(if is_compress { "Архивирование" } else { "Распаковка" }); 
                ui.separator();
                let (f, d) = self.count_limits();
                ui.label(format!("Файлы: {}/{} | Папки: {}/{}", f, MAX_SOURCE_FILES, d, MAX_SOURCE_FOLDERS));
            });
            ui.horizontal(|ui| { ui.label("Формат:"); ui.selectable_value(&mut self.format, ArchiveFormat::Zip, "ZIP"); ui.selectable_value(&mut self.format, ArchiveFormat::TarGz, "TAR.GZ"); ui.selectable_value(&mut self.format, ArchiveFormat::SevenZ, "7Z"); });
            
            egui::ScrollArea::vertical().max_height(120.0).show(ui, |ui| {
                for i in (0..self.sources.len()).rev() {
                    ui.horizontal(|ui| {
                        let src = &self.sources[i];
                        ui.label(format!("[{:?}] {}", if src.is_dir() {"DIR"} else {"FILE"}, src.file_name().unwrap_or_default().to_string_lossy()));
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.small_button("x").clicked() { 
                                self.sources.remove(i); 
                                self.update_destination_name(); 
                            }
                        });
                    });
                }
            });
            ui.horizontal(|ui| {
                if ui.button("Добавить файлы").clicked() {
                    if !self.can_add(false) { self.status = "Достигнут лимит файлов".into(); }
                    else if let Some(paths) = FileDialog::new().add_filter("Все файлы", &["*"]).pick_files() {
                        for p in paths { if self.can_add(false) && !self.sources.contains(&p) { self.sources.push(p); } else { break; } }
                        self.update_destination_name();
                    }
                }
                if ui.button("Добавить папки").clicked() {
                    if !self.can_add(true) { self.status = "Достигнут лимит папок".into(); }
                    else if let Some(p) = FileDialog::new().pick_folders() {
                        for f in p { if self.can_add(true) && !self.sources.contains(&f) { self.sources.push(f); } else { break; } }
                        self.update_destination_name();
                    }
                }
            });

            ui.horizontal(|ui| {
                let dest_label = if is_compress { "Файл назначения" } else { "Папка назначения" };
                if ui.button(dest_label).clicked() {
                    let path = if is_compress { FileDialog::new().save_file() } else { FileDialog::new().pick_folder() };
                    if let Some(p) = path { self.destination = Some(p); self.config.last_dest = self.destination.clone(); }
                }
                if let Some(p) = &self.destination { ui.label(p.display().to_string()); }
            });

            ui.separator();
            ui.horizontal(|ui| {
                ui.checkbox(&mut self.encrypt, "Шифровать");
                if self.encrypt { ui.add(egui::TextEdit::singleline(&mut self.password).password(!self.show_password).desired_width(120.0).hint_text("Пароль")); ui.checkbox(&mut self.show_password, "Показать"); }
            });
            if is_compress { ui.horizontal(|ui| { ui.label("Уровень:"); egui::ComboBox::from_label("").selected_text(self.level.to_string()).show_ui(ui, |ui| { for i in 0..=9 { ui.selectable_value(&mut self.level, i, i.to_string()); } }); }); }
            
            ui.separator(); ui.label(&self.status);
            egui::ProgressBar::new(self.progress).show_percentage().desired_width(400.0).ui(ui);
            ui.horizontal(|ui| { ui.label("Осталось:"); ui.label(&self.eta); });
            ui.horizontal(|ui| {
                if !self.is_busy {
                    if ui.button("Старт").clicked() { self.run_task(); }
                    if !is_compress && ui.button("Предпросмотр").clicked() { self.run_preview(); }
                    if self.is_success && ui.button("Открыть папку").clicked() { self.open_destination(); }
                    if ui.button("Очистить").clicked() { self.clear_selection(); }
                } else { if ui.button("Отмена").clicked() { self.cancel_task(); } }
            });
        });
        if self.show_preview {
            egui::Window::new("Содержимое").open(&mut self.show_preview).resizable(false).show(ctx, |ui| {
                if self.preview_loading { ui.spinner(); ui.label("Загрузка..."); return; }
                egui::ScrollArea::vertical().show(ui, |ui| {
                    egui::Grid::new("pv").striped(true).show(ui, |ui| {
                        ui.label("Имя"); ui.label("Размер"); ui.label("Тип"); ui.end_row();
                        for (n, s, d) in &self.preview_data { ui.label(n); ui.label(format!("{:.2} MB", *s as f64 / 1e6)); ui.label(if *d{"Папка"}else{"Файл"}); ui.end_row(); }
                    });
                });
            });
        }
        ctx.request_repaint();
    }
}