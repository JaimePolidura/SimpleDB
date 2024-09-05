pub struct DbOptions {
    storage_options: storage::lsm_options::LsmOptions,
    base_path: String,
}