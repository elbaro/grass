use slog::Logger;
use slog::{o, Drain};

pub fn create_logger(path: Option<&str>) -> Logger {
	let decorator = slog_term::TermDecorator::new().build();
	let drain_term = slog_term::CompactFormat::new(decorator).build().fuse();

	let logger = if let Some(path) = path {
		// "/tmp/grass.log"
		let file = std::fs::File::create(path).unwrap();
		let decorator = slog_term::PlainDecorator::new(file);
		let drain_file = slog_term::CompactFormat::new(decorator).build().fuse();

		let drain = slog::Duplicate(drain_term, drain_file).fuse();
		let drain = slog_async::Async::new(drain).build().fuse();

		slog::Logger::root(drain, o!())
	} else {
		let drain = slog_async::Async::new(drain_term).build().fuse();
		slog::Logger::root(drain, o!())
	};

	return logger;
}
