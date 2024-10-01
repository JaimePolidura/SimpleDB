use crate::SimpleDbOptions;
use std::sync::{Arc, OnceLock};
use log::{debug, error, info};

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
            let logger = Arc::new(Logger{ options });
            let _ = LOGGER.set(logger);
        }
    }

    pub fn info(&self, message: &str) {
        info!("{}", message);
    }

    pub fn error(&self, message: &str) {
        error!("{}", message);
    }

    pub fn debug(&self, message: &str) {
        if self.options.use_debug_logging {
            debug!("{}", message);
        }
    }
}