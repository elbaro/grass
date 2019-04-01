use crate::broker::{Broker, BrokerConfig};
use crate::worker::{Worker, WorkerConfig};
use slog::{error, info};

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
				broker: broker_config.map(|c| c.build()),
				worker: worker_config.map(|c| c.build()),
				life_token: AtomicCell::new(Some(life_token)),
				is_dead,
			}),
		}
	}

	pub fn run_sync(&self) {
		let inner = self.inner.clone();
		tokio::run_async(
			async move {
				let log = crate::logger::get_logger();
				error!(log, "[Daemon] run_async start");

				info!(log, "[Daemon] spawning broker/worker");

				if inner.broker.is_some() {
					let inner = inner.clone();
					tokio::spawn_async(
						async move {
							await!(inner.broker.as_ref().unwrap().run_async());
						},
					);
				}
				if inner.worker.is_some() {
					let inner = inner.clone();
					tokio::spawn_async(
						async move {
							await!(inner.worker.as_ref().unwrap().run_async());
						},
					);
				}

				// daemon RPC server
				use tokio::net::UnixListener;
				let listener = UnixListener::bind("/tmp/grass.sock").unwrap();
				let inner = inner.clone();

				let log2 = log.clone();
				let fut = listener
					.incoming()
					.take_until(inner.is_dead.clone()) // 0.1 stream
					.compat() // 0.3 stream
					.for_each(move |stream| {
						info!(log2, "[Daemon] new RPC connection");

						let stream = stream.unwrap();
						let transport = tarpc_bincode_transport::new(stream).fuse(); // fuse from Future03 ext trait
						let (sender, _recv) = futures::channel::mpsc::unbounded::<SocketAddr>();
						let channel = tarpc::server::Channel::new_simple_channel(transport, sender);
						channel.respond_with(serve(DaemonRPCServerImpl {
							daemon_inner: inner.clone(),
						})) // 0.3
					}); // 0.3
				// let fut = backward::Compat::new(fut.unit_error);

				await!(fut);
				error!(log, "[Daemon] run_sync done");
			},
		);
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

// use std::net::UnixStream;
use tokio::net::UnixStream;

pub async fn new_daemon_client() -> Result<Client, Box<dyn std::error::Error + 'static>> {
	// tarpc.server.incoming(0.3 stream of io::Result)
	// tarpc.server.incoming(bincode_transport:listen(addr))

	let tcp: UnixStream = await!(UnixStream::connect("/tmp/grass.sock").compat()).unwrap(); // futures03 convets future01 to Output=Result<>
																						// let transport = tarpc_bincode_transport::new(tcp);
	let transport = tarpc_bincode_transport::new(tcp);
	// let t:&tarpc::Transport = &transport;
	let client = await!(new_stub(tarpc::client::Config::default(), transport))?;
	Ok(client)
}
