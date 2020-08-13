use simplelog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};

pub fn init() {
    let filter;
    if cfg!(debug_assertions) {
        filter = LevelFilter::Debug;
    } else {
        filter = LevelFilter::Info;
    }

    let cfg = ConfigBuilder::new()
        .set_time_format_str("%H:%M:%S%.6f")
        .add_filter_allow_str("hydra")
        .add_filter_allow_str("mix")
        .add_filter_allow_str("directory_service")
        .build();

    TermLogger::init(filter, cfg, TerminalMode::Mixed).expect("Initializing Hydra logging failed");
}
