use simplelog::{LevelFilter, TermLogger, TerminalMode, ConfigBuilder};

pub fn init() {
    let filter;
    if cfg!(debug_assertions) {
        filter = LevelFilter::Debug;
    } else {
        filter = LevelFilter::Info;
    }

    let cfg = ConfigBuilder::new().set_time_format_str("%H:%M:%S%.6f").build();
    TermLogger::init(filter, cfg, TerminalMode::Mixed)
        .expect("Initializing Hydra logging failed");
}
