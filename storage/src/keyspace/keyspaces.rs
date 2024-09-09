use crate::keyspace::keyspace::Keyspace;
use crate::transactions::transaction_manager::TransactionManager;
use crossbeam_skiplist::SkipMap;
use std::cmp::max;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

pub struct Keyspaces {
    keyspaces: SkipMap<shared::KeyspaceId, Arc<Keyspace>>,
    next_keyspace_id: AtomicUsize,

    transaction_manager: Arc<TransactionManager>,
    options: Arc<shared::SimpleDbOptions>
}

impl Keyspaces {
    pub fn load_keyspaces(
        transaction_manager: Arc<TransactionManager>,
        options: Arc<shared::SimpleDbOptions>
    ) -> Result<Keyspaces, shared::SimpleDbError> {
        let keyspaces = SkipMap::new();
        let path = PathBuf::from(options.base_path.as_str());
        let path = path.as_path();
        let mut max_keyspace_id = 0;

        for file in fs::read_dir(path).map_err(|e| shared::SimpleDbError::CannotReadKeyspacesDirectories(e))? {
            let file = file.unwrap();
            if let Ok(keyspace_id) = file.file_name().to_str().unwrap().parse::<usize>() {
                let keyspace_id = keyspace_id as shared::KeyspaceId;
                let is_keyspace = file.metadata()
                    .map_err(|e| shared::SimpleDbError::CannotReadKeyspaceFile(keyspace_id, e))?
                    .is_dir();
                if is_keyspace {
                    let keyspace = Keyspace::create_and_load(
                        keyspace_id, transaction_manager.clone(), options.clone()
                    )?;
                    keyspaces.insert(keyspace_id, keyspace);
                    max_keyspace_id = max(max_keyspace_id, keyspace_id);
                }
            }
        }

        Ok(Keyspaces{
            next_keyspace_id: AtomicUsize::new(max_keyspace_id + 1),
            transaction_manager,
            options,
            keyspaces
        })
    }

    pub fn get_keyspace(&self, keyspace_id: shared::KeyspaceId) -> Result<Arc<Keyspace>, shared::SimpleDbError> {
        match self.keyspaces.get(&keyspace_id) {
            Some(entry) => Ok(entry.value().clone()),
            None => Err(shared::SimpleDbError::KeyspaceNotFound(keyspace_id))
        }
    }

    pub fn create_keyspace(&self) -> Result<Arc<Keyspace>, shared::SimpleDbError> {
        let keyspace_id = self.next_keyspace_id.fetch_add(1, Relaxed) as shared::KeyspaceId;
        let keyspace = Keyspace::create_new(keyspace_id, self.transaction_manager.clone(), self.options.clone())?;
        self.keyspaces.insert(keyspace_id, keyspace.clone());
        Ok(keyspace)
    }

    pub fn has_txn_id_been_written(&self, txn_id: shared::TxnId) -> bool {
        for keyspace in self.keyspaces.iter() {
            let keyspace = keyspace.value();
            if keyspace.has_txn_id_been_written(txn_id) {
                return true;
            }
        }

        false
    }

    pub fn start_keyspaces_compaction_threads(&self) {
        for keyspace in self.keyspaces.iter() {
            let keyspace = keyspace.value();
            keyspace.start_compaction_thread();
        }
    }

    pub fn recover_from_manifest(&mut self) {
        for keyspace in self.keyspaces.iter() {
            let keyspace = keyspace.value();
            keyspace.recover_from_manifest();
        }
    }
}