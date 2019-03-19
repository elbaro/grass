use once_cell::sync::OnceCell;
use slog::Logger;
use slog::{o, Drain};
use std::sync::Mutex;

static LOGGER: OnceCell<Mutex<Logger>> = OnceCell::INIT;

pub fn init_logger(path: Option<&str>) -> Logger {
	let decorator = slog_term::TermDecorator::new().build();
	let drain_term = slog_term::CompactFormat::new(decorator).build().fuse();

	let logger = if let Some(path) = path {
		// "/tmp/grass.log"
		let file = std::fs::File::open(path).unwrap();
		let decorator = slog_term::PlainDecorator::new(file);
		let drain_file = slog_term::CompactFormat::new(decorator).build().fuse();

		let drain = slog::Duplicate(drain_term, drain_file).fuse();
		let drain = slog_async::Async::new(drain).build().fuse();

		slog::Logger::root(drain, o!())
	} else {
		let drain = slog_async::Async::new(drain_term).build().fuse();
		slog::Logger::root(drain, o!())
	};

	LOGGER.set(Mutex::new(logger)).unwrap();
	get_logger()
}

pub fn get_logger() -> Logger {
	LOGGER
		.get()
		.expect("logger is not initialized")
		.lock()
		.unwrap()
		.clone()
}
