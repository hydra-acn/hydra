use simplelog::{LevelFilter, TermLogger, TerminalMode};

pub fn init() {
    let filter;
    if cfg!(debug_assertions) {
        filter = LevelFilter::Debug;
    } else {
        filter = LevelFilter::Info;
    }

    TermLogger::init(
        filter,
        simplelog::Config::default(),
        TerminalMode::Mixed,
    )
    .expect("Initializing Hydra logging failed");
}
