use std::fs::DirEntry;

pub(crate) fn is_sstable_file(file: &DirEntry) -> bool {
    file.file_name().to_str().unwrap().starts_with("sst-")
}

pub(crate) fn to_sstable_file_name(sstable_id: usize) -> String {
    let result = format!("sst-{}", sstable_id);
    result
}

pub(crate) fn extract_sstable_id_from_file(file: &DirEntry) -> Result<usize, ()> {
    let file_name = file.file_name();
    let split = file_name
        .to_str()
        .unwrap()
        .split("-")
        .last();

    if split.is_some() {
        return split.unwrap()
            .parse::<usize>()
            .map_err(|e| ());
    } else {
        Err(())
    }
}
