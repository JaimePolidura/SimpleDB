use serde_json::from_slice;
use shared::{start_simpledb_options_builder_from, SimpleDbFile, SimpleDbFileMode, SimpleDbOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn load_options(
    path: String
) -> Result<Arc<SimpleDbOptions>, ()> {
    let path = Path::new(path.as_str());
    if path.exists() {
        match load_options_from_existing_file(path) {
            Ok(options) => {
                let options = add_path_to_options(options, path);
                Ok(Arc::new(options))
            }
            Err(_) => {
                //Reset to default settings
                populate_options_file_with_default_data(path);
                let options = add_path_to_options(SimpleDbOptions::default(), path);
                Ok(Arc::new(options))
            }
        }
    } else {
        populate_options_file_with_default_data(path);
        let options = add_path_to_options(SimpleDbOptions::default(), path);
        Ok(Arc::new(options))
    }
}

fn populate_options_file_with_default_data(base_path: &Path) {
    let config_file_path = config_file_path(base_path);
    let config_file_path = config_file_path.as_path();

    let mut file = SimpleDbFile::create(config_file_path, &Vec::new(), SimpleDbFileMode::RandomWrites)
        .expect("Cannot create options file");
    let serialized = serde_json::to_string_pretty(&SimpleDbOptions::default());
    let serialized = serialized.unwrap().as_bytes().to_vec();
    file.write(&serialized);
    file.fsync();
}

fn load_options_from_existing_file(base_path: &Path) -> Result<SimpleDbOptions, ()> {
    let config_file_path = config_file_path(base_path);
    let config_file_path = config_file_path.as_path();

    let mut file = SimpleDbFile::create(config_file_path, &Vec::new(), SimpleDbFileMode::RandomWrites)
        .expect("Cannot create options file");
    let bytes = file.read_all()
        .expect("Cannot read file bytes");
    let options: SimpleDbOptions = from_slice(&bytes)
        .map_err(|_| ())?;
    Ok(options)
}

fn add_path_to_options(options: SimpleDbOptions, path: &Path) -> SimpleDbOptions {
    start_simpledb_options_builder_from(&options)
        .base_path(path.to_str().unwrap())
        .build()
}

fn config_file_path(base_path: &Path) -> PathBuf {
    let mut path_buff = PathBuf::from(base_path);
    path_buff.push("config.json");
    path_buff
}