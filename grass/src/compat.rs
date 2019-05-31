use futures::compat::Compat;
use futures::compat::Executor01CompatExt;
use futures::{Future, FutureExt, TryFutureExt};
use slog::crit;

pub fn tokio_run<F: Future<Output=O> + Send + 'static, O: Send + 'static>(future: F) -> O {
	tarpc::init(tokio::executor::DefaultExecutor::current().compat());
    let runtime = tokio::runtime::Runtime::new().expect("failed to start new Runtime");
	runtime.block_on_all(Compat::new(Box::pin(
		future.map(|output| -> Result<_, failure::Error> { Ok(output) }),
	))).unwrap()
}

pub fn tokio_try_run<F: Future<Output = Result<O, failure::Error>> + Send + 'static, O: Send+'static>(future: F) -> Result<O, failure::Error> {
	tarpc::init(tokio::executor::DefaultExecutor::current().compat());
    let runtime = tokio::runtime::Runtime::new().expect("failed to start new Runtime");
	runtime.block_on_all(Compat::new(Box::pin(future)))
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
