use crate::broker::{Broker, BrokerConfig};
use crate::worker::{Worker, WorkerConfig};
use slog::{error, info};

use futures::compat::Compat;
use futures::compat::Executor01CompatExt;
use futures::compat::Future01CompatExt;
use futures::compat::Stream01CompatExt;
use futures::{Future, FutureExt, Stream, StreamExt, TryFutureExt};
use futures01::Stream as Stream01;

use tokio_async_await::compat::backward;

use std::net::SocketAddr;
use std::sync::Arc;

use crate::oneshot::StreamExt as OneshotStreamExt;

use crossbeam::atomic::AtomicCell;

tarpc::service! {
	rpc stop();
}

pub struct Daemon {
	// inner+Arc is required because tokio needs 'static
	inner: Arc<DaemonInner>,
}

struct DaemonInner {
	pub broker: Option<Broker>, // optionally has bind addr
	pub worker: Option<Worker>, // optionally has brok addr

	stopper: crate::oneshot::OneshotSender<()>,
	stop_flag: crate::oneshot::OneshotFlag<()>,
}

impl Drop for DaemonInner {
	fn drop(&mut self) {
		self.stop();
	}
}

impl Daemon {
	pub fn new(broker_config: Option<BrokerConfig>, worker_config: Option<WorkerConfig>) -> Daemon {
		let (stopper, stop_flag) = crate::oneshot::new::<()>();
		Daemon {
			inner: Arc::new(DaemonInner {
				broker: broker_config.map(|c| c.build(stop_flag.clone())),
				worker: worker_config.map(|c| c.build(stop_flag.clone())),
				stopper: stopper,
				stop_flag,
			}),
		}
	}

	pub fn run_sync(&self) {
		tarpc::init(tokio::executor::DefaultExecutor::current().compat());
		let inner = self.inner.clone();
		crate::compat::tokio_run(
			async move {
				let log = slog_scope::logger();
				info!(log, "[Daemon] Running.");

				if inner.broker.is_some() {
					let inner = inner.clone();
					crate::compat::tokio_spawn(
						async move {
							await!(inner.broker.as_ref().unwrap().run_async());
						},
					);
				}
				if inner.worker.is_some() {
					let inner = inner.clone();
					crate::compat::tokio_spawn(
						async move {
							await!(inner.worker.as_ref().unwrap().run_async()).unwrap();
						},
					);
				}

				// daemon RPC server
				use tokio::net::UnixListener;

				let listener = UnixListener::bind("/tmp/grass.sock").unwrap();
				let inner = inner.clone();

				let mut listener = listener
					.incoming()
					.compat()
					.take_until(inner.stop_flag.clone().map(|_| ()));

				while let Some(stream) = await!(listener.next()) {
					info!(log, "[Daemon] new RPC connection");

					clone_all::clone_all!(log, inner);
					crate::compat::tokio_spawn(
						async move {
							let stream = stream.unwrap();
							let transport = tarpc_bincode_transport::new(stream).fuse(); // fuse from Future03 ext trait
							let (sender, _recv) = futures::channel::mpsc::unbounded::<SocketAddr>();
							let channel =
								tarpc::server::Channel::new_simple_channel(transport, sender);
							await!(channel.respond_with(serve(DaemonRPCServerImpl {
								daemon_inner: inner.clone(),
							})));

							info!(log, "[Daemon] Connection closed");
						},
					);
				}
			},
		);
		let log = slog_scope::logger();
		info!(log, "[Daemon] exit");
	}
}

impl DaemonInner {
	pub fn stop(&self) {
		let _ = self.stopper.send(());
	}
}

#[derive(Clone)]
struct DaemonRPCServerImpl {
	daemon_inner: Arc<DaemonInner>,
}

// Daemon <-> CLI RPC
use futures::future;
use futures::future::Ready;

use tarpc::context;

impl Service for DaemonRPCServerImpl {
	type StopFut = Ready<()>;
	fn stop(self, _: context::Context) -> Self::StopFut {
		// stop broker
		// stop worker
		let log = slog_scope::logger();
		info!(log, "[Daemon] stop()");
		self.daemon_inner.stop();
		future::ready(())
	}
}

use tokio::net::UnixStream;
use tokio::prelude::*;

pub async fn new_daemon_client() -> Result<Client, Box<dyn std::error::Error + 'static>> {
	let log = slog_scope::logger();
	info!(log, "[Client] Connecting to Daemon.");
	let tcp: UnixStream = await!(UnixStream::connect("/tmp/grass.sock").compat()).unwrap();
	let transport = tarpc_bincode_transport::Transport::from(tcp);
	let client = await!(new_stub(tarpc::client::Config::default(), transport))?;
	info!(log, "[Client] Connected to Daemon.");
	Ok(client)
}
