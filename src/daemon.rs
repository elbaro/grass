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

use stream_cancel::StreamExt as StreamCancel;

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

	life_token: AtomicCell<Option<stream_cancel::Trigger>>,
	is_dead: stream_cancel::Tripwire,
}

impl Drop for DaemonInner {
	fn drop(&mut self) {
		self.stop();
	}
}

impl Daemon {
	pub fn new(broker_config: Option<BrokerConfig>, worker_config: Option<WorkerConfig>) -> Daemon {
		let (life_token, is_dead) = stream_cancel::Tripwire::new();
		Daemon {
			inner: Arc::new(DaemonInner {
				broker: broker_config.map(|c| c.build(is_dead.clone())),
				worker: worker_config.map(|c| c.build(is_dead.clone())),
				life_token: AtomicCell::new(Some(life_token)),
				is_dead,
			}),
		}
	}

	pub fn run_sync(&self) {
		tarpc::init(tokio::executor::DefaultExecutor::current().compat());
		let inner = self.inner.clone();
		crate::compat::tokio_run(
			async move {
				let log = crate::logger::get_logger();
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
					.take_until(inner.is_dead.clone()) // 0.1 stream
					.compat();

				while let Some(stream) = await!(listener.next()) {
					info!(log, "[Daemon] new RPC connection");

					clone_all::clone_all!(log, inner);
					crate::compat::tokio_spawn(
						async move {
							let stream = stream.unwrap();
							let transport = tarpc_bincode_transport::new(stream).fuse(); // fuse from Future03 ext trait
							let (sender, _recv) =
								futures::channel::mpsc::unbounded::<SocketAddr>();
							let channel =
								tarpc::server::Channel::new_simple_channel(transport, sender);
							await!(channel.respond_with(serve(DaemonRPCServerImpl {
								daemon_inner: inner.clone(),
							})));

							info!(log, "[Daemon] Connection closed");
						},
					);
				};
			},
		);
		let log = crate::logger::get_logger();
		info!(log, "[Daemon] Exit.");
	}
}

impl DaemonInner {
	pub fn stop(&self) {
		self.life_token.store(None);
		if let Some(broker) = self.broker.as_ref() {
			broker.stop();
		}
		if let Some(worker) = self.worker.as_ref() {
			worker.stop();
		}
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
		let log = crate::logger::get_logger();
		info!(log, "[Daemon] stop()");
		self.daemon_inner.stop();
		future::ready(())
	}
}

use tokio::net::UnixStream;
use tokio::prelude::*;

pub async fn new_daemon_client() -> Result<Client, Box<dyn std::error::Error + 'static>> {
	let log = crate::logger::get_logger();
	info!(log, "[Client] Connecting to Daemon.");
	let tcp: UnixStream = await!(UnixStream::connect("/tmp/grass.sock").compat()).unwrap();
	let transport = tarpc_bincode_transport::Transport::from(tcp);
	let client = await!(new_stub(tarpc::client::Config::default(), transport))?;
	info!(log, "[Client] Connected to Daemon.");
	Ok(client)
}
