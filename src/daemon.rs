use crate::broker::{Broker, BrokerConfig};
use crate::rpc::DaemonRPCClient;
use crate::worker::{Worker, WorkerConfig};
use essrpc::transports::BincodeTransport;
use essrpc::RPCClient;
use essrpc::RPCServer;
use slog::{error, info};

use futures::future::Future;
use futures::stream::Stream;
use std::sync::Arc;

use stream_cancel::StreamExt;

use crossbeam::atomic::AtomicCell;

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
			})
		}
	}

	pub fn run_sync(&self) {
		crossbeam::scope(|scope| {
			let log = crate::logger::get_logger();
			info!(log, "[Daemon] spawning broker/worker");

			if self.inner.broker.is_some() {
				let inner = self.inner.clone();
				scope.spawn(move |_| inner.broker.as_ref().unwrap().run_sync());
			}
			if self.inner.worker.is_some() {
				let inner = self.inner.clone();
				scope.spawn(move |_| inner.worker.as_ref().unwrap().run_sync());
			}

			// daemon RPC server
			use tokio::net::UnixListener;
			let listener = UnixListener::bind("/tmp/grass.sock").unwrap();
			let daemon_inner = self.inner.clone();
			let log2 = log.clone();
			let fut = listener
				.incoming()
				.take_until(self.inner.is_dead.clone())
				.for_each(move |stream| {
					info!(log, "[Daemon] new RPC connection");

					let log = log.clone();
					// let daemon_inner = daemon_inner.clone();
					// std::thread::spawn(move || {
					// 	let transport = BincodeTransport::new(stream);

					// 	let mut s = crate::rpc::DaemonRPCServer::new(
					// 		DaemonRPCServerImpl { daemon_inner: daemon_inner.clone() },
					// 		transport,
					// 	);
					// 	let result = s.serve(); // err on normal disconnect
					// 	if result.is_err() {
					// 		error!(log, "RPC finished"; "result"=>?result);
					// 	}
					// });



					let ch = tarpc::server::Channel {

					};

					futures::future::ok(())
				})
				.map_err(move |err| {
					error!(log2, "UnixListener error";"err"=>%err);
				});

			tokio::run(fut);
		})
		.unwrap();
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

struct DaemonRPCServerImpl {
	daemon_inner: Arc<DaemonInner>,
}

impl crate::rpc::Daemon for DaemonRPCServerImpl {
	fn stop(&self) -> Result<(), essrpc::RPCError> {
		// stop broker
		// stop worker
		let log = crate::logger::get_logger();
		info!(log, "[Daemon] stop()");
		self.daemon_inner.stop();
		Ok(())
	}
}

// use std::net::UnixStream;
use std::os::unix::net::UnixStream;

pub fn new_daemon_client(
) -> Result<DaemonRPCClient<BincodeTransport<UnixStream>>, Box<std::error::Error>> {

	let stream = UnixStream::connect("/tmp/grass.sock")?;
	Ok(DaemonRPCClient::new(BincodeTransport::new(stream)))
}
