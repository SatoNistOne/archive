mod app;
mod config;
mod archiver;

use std::path::PathBuf;
use app::ArchiverApp;

fn main() -> eframe::Result {
    let args: Vec<String> = std::env::args().collect();
    let initial_path = if args.len() > 1 { Some(PathBuf::from(&args[1])) } else { None };

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([540.0, 480.0])
            .with_resizable(true)
            .with_title("Rust Archiver"),
        ..Default::default()
    };

    eframe::run_native(
        "Rust Archiver",
        options,
        Box::new(|cc| Ok(Box::new(ArchiverApp::new(cc, initial_path)))),
    )
}