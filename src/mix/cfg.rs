use config::{ConfigError, File};
use serde::Deserialize;

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct FirebaseConfig {
    pub server_auth_key: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub firebase: FirebaseConfig,
}

impl Config {
    pub fn new(cfg_file: Option<&str>) -> Result<Self, ConfigError> {
        let mut cfg = config::Config::new();
        if let Some(f) = cfg_file {
            cfg.merge(File::with_name(f))?;
        }
        cfg.try_into()
    }
}
