use crate::keyspace::keyspace::{Keyspace};
use crate::lsm_error::LsmError;
use crate::lsm_error::LsmError::{CannotReadKeyspaceFile, CannotReadKeyspacesDirectories, KeyspaceNotFound};
use crate::lsm_options::LsmOptions;
use crate::transactions::transaction_manager::TransactionManager;
use crate::utils::utils;
use crossbeam_skiplist::SkipMap;
use std::cmp::max;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use crate::lsm::KeyspaceId;
use crate::transactions::transaction::TxnId;

pub struct Keyspaces {
    keyspaces: SkipMap<KeyspaceId, Arc<Keyspace>>,
    next_keyspace_id: AtomicUsize,

    transaction_manager: Arc<TransactionManager>,
    lsm_options: Arc<LsmOptions>
}

impl Keyspaces {
    pub fn load_keyspaces(
        transaction_manager: Arc<TransactionManager>,
        lsm_options: Arc<LsmOptions>
    ) -> Result<Keyspaces, LsmError> {
        let keyspaces = SkipMap::new();
        let path = PathBuf::from(lsm_options.base_path.as_str());
        let path = path.as_path();
        let mut max_keyspace_id = 0;

        for file in fs::read_dir(path).map_err(|e| CannotReadKeyspacesDirectories(e))? {
            let file = file.unwrap();
            if let Ok(keyspace_id) = file.file_name().to_str().unwrap().parse::<usize>() {
                let keyspace_id = keyspace_id as KeyspaceId;
                let is_keyspace = file.metadata()
                    .map_err(|e| CannotReadKeyspaceFile(keyspace_id, e))?
                    .is_dir();
                if is_keyspace {
                    let keyspace = Keyspace::load(
                        keyspace_id, transaction_manager.clone(), lsm_options.clone()
                    )?;
                    keyspaces.insert(keyspace_id, keyspace);
                    max_keyspace_id = max(max_keyspace_id, keyspace_id);
                }
            }
        }

        Ok(Keyspaces{
            next_keyspace_id: AtomicUsize::new(max_keyspace_id + 1),
            transaction_manager,
            lsm_options,
            keyspaces
        })
    }

    pub fn get_keyspace(&self, keyspace_id: KeyspaceId) -> Result<Arc<Keyspace>, LsmError> {
        match self.keyspaces.get(&keyspace_id) {
            Some(entry) => Ok(entry.value().clone()),
            None => Err(KeyspaceNotFound(keyspace_id))
        }
    }

    pub fn create_keyspace(&self) -> Result<Arc<Keyspace>, LsmError> {
        let keyspace_id = self.next_keyspace_id.fetch_add(1, Relaxed) as KeyspaceId;
        let keyspace = Keyspace::create_new(keyspace_id, self.transaction_manager.clone(), self.lsm_options.clone())?;
        self.keyspaces.insert(keyspace_id, keyspace.clone());
        Ok(keyspace)
    }

    pub fn has_txn_id_been_written(&self, txn_id: TxnId) -> bool {
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