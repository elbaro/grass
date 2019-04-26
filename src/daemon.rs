use crate::broker::{Broker, BrokerConfig};
use crate::objects::QueueCapacity;
use crate::worker::{QueueConfig, Worker, WorkerConfig};

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use failure::ResultExt;
use serde::{Deserialize, Serialize};
use slog::{error, info};

use futures::compat::Compat;
use futures::compat::Executor01CompatExt;
use futures::compat::Future01CompatExt;
use futures::compat::Stream01CompatExt;
use futures::{Future, FutureExt, Stream, StreamExt, TryFutureExt};
use futures01::Stream as Stream01;

use crate::oneshot::StreamExt as OneshotStreamExt;

tarpc::service! {
	rpc stop();
	rpc create_queue(config: QueueConfig);
	rpc delete_queue(name:String);
	rpc info() -> DaemonInfo;
}

pub struct Daemon {
	// inner+Arc is required because tokio needs 'static
	pub inner: Arc<DaemonInner>,
}

pub struct DaemonInner {
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
				stopper,
				stop_flag,
			}),
		}
	}

	pub fn run_sync(&self) {
		tarpc::init(tokio::executor::DefaultExecutor::current().compat());
		let inner = self.inner.clone();
		crate::compat::tokio_try_run(
			async move {
				let log = slog_scope::logger();
				info!(log, "[Daemon] Running.");

				if inner.broker.is_some() {
					let inner = inner.clone();
					crate::compat::tokio_try_spawn(
						async move {
							await!(inner
								.broker
								.as_ref()
								.ok_or(failure::err_msg("no broker component"))?
								.run_async())
							.context("error from broker")?;
							Ok(())
						},
					);
				}
				if inner.worker.is_some() {
					let inner = inner.clone();
					crate::compat::tokio_try_spawn(
						async move {
							await!(inner
								.worker
								.as_ref()
								.ok_or(failure::err_msg("no worker component"))?
								.run_async())
							.context("error from worker")?;
							Ok(())
						},
					);
				}

				// daemon RPC server
				use tokio::net::UnixListener;

				let listener = UnixListener::bind("/tmp/grass.sock")
					.context("Fail to bind /tmp/grass.sock")?;
				let inner = inner.clone();

				let mut listener = listener
					.incoming()
					.compat()
					.take_until(inner.stop_flag.clone().map(|_| ()));

				// let der = {
				// 	let mut file =
				// 		std::fs::File::open("	keyStore.p12").context("p12 file does not exist")?;
				// 	let mut bytes = vec![];
				// 	file.read_to_end(&mut bytes)
				// 		.context("Fail to read .p12 key")?;
				// 	bytes
				// };

				// let cert = native_tls::Identity::from_pkcs12(&der[..], "1234")
				// 	.context("Wrong passphrase for .p12")?;
				// let tls_acceptor = native_tls::TlsAcceptor::builder(cert)
				// 	.build()
				// 	.context("Error building native_tls::TlsAcceptor")?;
				// let tls_acceptor = tokio_tls::TlsAcceptor::from(tls_acceptor);

				while let Some(stream) = await!(listener.next()) {
					info!(log, "[Daemon] new RPC connection");

					clone_all::clone_all!(log, inner
						// ,tls_acceptor
					);
					crate::compat::tokio_try_spawn(
						async move {
							let stream = stream.context("cannot open tcp stream")?;
							// let stream = await!(tls_acceptor.accept(stream).compat())
							// 	.context("fail to handshake tls")?;

							let transport = tarpc_bincode_transport::new(stream).fuse(); // fuse from Future03 ext trait
							let (sender, _recv) = futures::channel::mpsc::unbounded::<SocketAddr>();
							let channel =
								tarpc::server::Channel::new_simple_channel(transport, sender);
							await!(channel.respond_with(serve(DaemonRPCServerImpl {
								daemon_inner: inner.clone(),
							})));

							info!(log, "[Daemon] Connection closed");
							Ok(())
						},
					);
				}

				Ok(())
			},
		).unwrap();
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

	type CreateQueueFut = Pin<Box<dyn Future<Output = ()> + Send>>;
	fn create_queue(self, _: context::Context, config: QueueConfig) -> Self::CreateQueueFut {
		Box::pin(
			async move {
				if let Some(worker) = self.daemon_inner.worker.as_ref() {
					await!(worker.create_queue(config));
				}
			},
		)
	}

	type DeleteQueueFut = Pin<Box<dyn Future<Output = ()> + Send>>;
	fn delete_queue(self, _: context::Context, name: String) -> Self::DeleteQueueFut {
		Box::pin(
			async move {
				if let Some(worker) = self.daemon_inner.worker.as_ref() {
					await!(worker.delete_queue(name));
				}
			},
		)
	}

	type InfoFut = Pin<Box<dyn Future<Output = DaemonInfo> + Send>>;
	fn info(self, _: context::Context) -> Self::InfoFut {
		Box::pin(async move { await!(DaemonInfo::from(self.daemon_inner.clone())) })
	}
}

use tokio::net::UnixStream;
use tokio::prelude::*;

pub async fn new_daemon_client() -> Result<Client, failure::Error> {
	let log = slog_scope::logger();
	info!(log, "[Client] Connecting to Daemon.");
	let tcp: UnixStream = await!(UnixStream::connect("/tmp/grass.sock").compat())
		.context("Could not connect to Daemon")?;
	// let tls_connector = tokio_tls::TlsConnector::from(
	// 	native_tls::TlsConnector::builder()
	// 		.use_sni(false)
	// 		.danger_accept_invalid_hostnames(true)
	// 		.danger_accept_invalid_certs(true)
	// 		.build()
	// 		.context("cannot build tls connector")?,
	// );
	// let tcp =
	// 	await!(tls_connector.connect("domain", tcp).compat()).context("cannot establish TLS")?;
	let transport = tarpc_bincode_transport::Transport::from(tcp);
	let client = await!(new_stub(tarpc::client::Config::default(), transport))?;
	info!(log, "[Client] Connected to Daemon.");
	Ok(client)
}

use crate::broker::BrokerInfo;
use crate::worker::WorkerInfo;

#[derive(Debug, Serialize, Deserialize)]
pub struct DaemonInfo {
	pub broker: bool,
	pub worker: Option<WorkerInfo>,
}

impl DaemonInfo {
	pub async fn from(daemon: Arc<DaemonInner>) -> DaemonInfo {
		DaemonInfo {
			broker: daemon.broker.is_some(),
			// if let Some(broker) = daemon.broker.as_ref() {
			// 	Some(await!(BrokerInfo::from(broker.inner.clone())))
			// } else {
			// 	None
			// },
			worker: if let Some(worker) = daemon.worker.as_ref() {
				Some(await!(WorkerInfo::from(worker.inner.clone())))
			} else {
				None
			},
		}
	}
}
