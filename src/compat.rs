use futures::compat::Compat;
use futures::compat::Executor01CompatExt;
use futures::{Future, FutureExt, TryFutureExt};
use slog::crit;

pub fn tokio_run<F: Future<Output = ()> + Send + 'static>(future: F) {
	tarpc::init(tokio::executor::DefaultExecutor::current().compat());
	tokio::run(Compat::new(Box::pin(
		future.map(|()| -> Result<(), ()> { Ok(()) }),
	)));
}

pub fn tokio_try_run<F: Future<Output = Result<(), failure::Error>> + Send + 'static>(future: F) {
	tarpc::init(tokio::executor::DefaultExecutor::current().compat());
	tokio::run(Compat::new(Box::pin(future.map_err(|err| {
		let log = slog_scope::logger();
		crit!(log, "Error in tokio_run"; "err"=>%err);
		panic!();
	}))));
}

pub fn tokio_spawn<F: Future<Output = ()> + Send + 'static>(future: F) {
	tokio::spawn(Compat::new(Box::pin(
		future.map(|()| -> Result<(), ()> { Ok(()) }),
	)));
}

pub fn tokio_try_spawn<F: Future<Output = Result<(), failure::Error>> + Send + 'static>(future: F) {
	tokio::spawn(Compat::new(Box::pin(future.map_err(|err| {
		let log = slog_scope::logger();
		crit!(log, "Error in tokio_spawn"; "err"=>%err);
		panic!();
	}))));
}
