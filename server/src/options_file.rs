use serde_json::{from_slice, to_vec};
use shared::{start_simpledb_options_builder_from, SimpleDbFile, SimpleDbFileMode, SimpleDbOptions};
use std::path::Path;
use std::sync::Arc;

pub fn load_options(
    path: String
) -> Result<Arc<SimpleDbOptions>, ()> {
    let path = Path::new(path.as_str());
    if path.exists() {
        let options = load_options_from_existing_file(path);
        let options = add_path_to_options(options, path);
        Ok(Arc::new(options))
    } else {
        populate_options_file_with_default_data(path);
        let options = add_path_to_options(SimpleDbOptions::default(), path);
        Ok(Arc::new(options))
    }
}

fn populate_options_file_with_default_data(path: &Path) {
    let mut file = SimpleDbFile::create(path, &Vec::new(), SimpleDbFileMode::RandomWrites)
        .expect("Cannot create options file");
    let serialized = to_vec(&SimpleDbOptions::default()).unwrap();
    file.write(&serialized);
    file.fsync();
}

fn load_options_from_existing_file(path: &Path) -> SimpleDbOptions {
    let mut file = SimpleDbFile::create(path, &Vec::new(), SimpleDbFileMode::RandomWrites)
        .expect("Cannot create options file");
    let bytes = file.read_all()
        .expect("Cannot read file bytes");
    let options: SimpleDbOptions = from_slice(&bytes).unwrap();
    options
}

fn add_path_to_options(options: SimpleDbOptions, path: &Path) -> SimpleDbOptions {
    start_simpledb_options_builder_from(&options)
        .base_path(path.to_str().unwrap())
        .build()
}