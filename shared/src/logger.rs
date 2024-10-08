use crate::{KeyspaceId, SimpleDbOptions};
use std::sync::{Arc, OnceLock};
use env_logger::Builder;
use log::{debug, error, info};

pub enum SimpleDbLayer {
    Server,
    DB(String), //Table name4
    StorageKeyspace(KeyspaceId),
    Storage,
}

static LOGGER: OnceLock<Arc<Logger>> = OnceLock::new();

pub struct Logger {
    options: Arc<SimpleDbOptions>,
}

pub fn logger() -> Arc<Logger> {
    LOGGER.get().expect("Logger is not initialized").clone()
}

impl Logger {
    pub fn init(options: Arc<SimpleDbOptions>) {
        if LOGGER.get().is_none() {
            let mut builder = Builder::new();
            builder.filter_level(log::LevelFilter::Info);
            builder.filter_level(log::LevelFilter::Debug);
            builder.init();

            let logger = Arc::new(Logger{ options });
            let _ = LOGGER.set(logger);
        }
    }

    pub fn info(&self, layer: SimpleDbLayer, message: &str) {
        info!("[{}] {}", layer.to_string(), message);
    }

    pub fn error(&self, layer: SimpleDbLayer, message: &str) {
        error!("[{}] {}", layer.to_string(), message);
    }

    pub fn debug(&self, layer: SimpleDbLayer, message: &str) {
        if self.options.use_debug_logging {
            debug!("[{}] {}", layer.to_string(), message);
        }
    }
}

impl SimpleDbLayer {
    pub fn to_string(&self) -> String {
        match self {
            SimpleDbLayer::Server => "Server".to_string(),
            SimpleDbLayer::DB(table_name) => format!("DB Table: {}", table_name),
            SimpleDbLayer::StorageKeyspace(keyspace_id) => format!("Storage Keyspace ID: {}", keyspace_id),
            SimpleDbLayer::Storage => "Storage".to_string()
        }
    }
}