use simplelog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};

pub fn init(log_external: bool) {
    let filter;
    if cfg!(debug_assertions) {
        filter = LevelFilter::Debug;
    } else {
        filter = LevelFilter::Info;
    }

    let mut builder = ConfigBuilder::new();
    builder.set_time_format_str("%H:%M:%S%.6f");
    if log_external == false {
        builder
            .add_filter_allow_str("hydra")
            .add_filter_allow_str("mix")
            .add_filter_allow_str("directory_service");
    }
    let cfg = builder.build();

    TermLogger::init(filter, cfg, TerminalMode::Mixed).expect("Initializing Hydra logging failed");
}
