use crate::objects::{Job, JobSpecification, JobStatus, WorkerCapacity, WorkerInfo};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use futures::compat::Compat;
use futures::compat::Executor01CompatExt;
use futures::compat::Future01CompatExt;
use futures::compat::Stream01CompatExt;
use futures::lock::Mutex;
use futures::{future::Ready, Future, FutureExt, Stream, StreamExt, TryFutureExt};
use futures01::stream::Stream as Stream01;
use futures01::Future as Future01;

use tarpc::context;
use tarpc::server::Handler;

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

#[allow(unused_imports)]
use slog::{error, info, warn};

use serde::{Deserialize, Serialize};

pub struct BrokerConfig {
	pub bind_addr: SocketAddr,
}

impl BrokerConfig {
	pub fn build(self, stop_flag: crate::oneshot::OneshotFlag) -> Broker {
		Broker {
			inner: Arc::new(BrokerInner {
				bind_addr: self.bind_addr,
				workers: Default::default(),
				jobs: Default::default(),
				pending_job_ids: Default::default(),
				stop_flag,
			}),
		}
	}
}

tarpc::service! {
	rpc ping();
	rpc job_update(job_id: String, status: JobStatus);
	rpc job_request(capacity: WorkerCapacity) -> Option<Job>;
	rpc job_enqueue(spec: JobSpecification);
	// rpc job_enqueue(spec: String);
	rpc info() -> BrokerInfo;
}

pub struct Broker {
	// reason for inner pattern
	inner: Arc<BrokerInner>,
}

impl Broker {
	pub fn stop(&self) {
		self.inner.stop();
	}
	pub async fn run_async(&self) {
		let log = crate::logger::get_logger();
		info!(log, "[Broker] Listening."; "bind"=>%self.inner.bind_addr);

		let listener = TcpListener::bind(&self.inner.bind_addr).unwrap();

		// let mut serving = listener.incoming().compat();
		use crate::oneshot::StreamExt as OneshotStreamExt;
		let mut serving = listener
			.incoming()
			.compat()
			.take_until(self.inner.stop_flag.clone().map(|_| ()));

		let inner = self.inner.clone();
		while let Some(stream) = await!(serving.next()) {
			clone_all::clone_all!(log, inner);
			crate::compat::tokio_spawn(
				async move {
					// let inner = (&inner).clone();
					let session_id = uuid::Uuid::new_v4().to_hyphenated().to_string();
					info!(log, "[Broker] New session";"id"=>&session_id);
					let stream = stream.unwrap();

					// yamux
					let mut mux = yamux::Connection::new(
						stream,
						yamux::Config::default(),
						yamux::Mode::Server,
					);
					let stream = await!(mux.next()).unwrap().unwrap();

					// stream1: rpc server
					let transport = tarpc_bincode_transport::new(stream).fuse(); //.fuse(∂);  // fuse from Future03 ext trait
					let (sender, _recv) = futures::channel::mpsc::unbounded::<SocketAddr>();
					let channel = tarpc::server::Channel::new_simple_channel(transport, sender);

					let mut session_serve = channel
						.respond_with(serve(BrokerRPCServerImpl {
							broker: inner.clone(),
						}))
						.fuse();

					// stream2: client (optional)
					let mut fut2 = Box::pin(
						async {
							if let Some(Ok(conn2)) = await!(mux.next()) {
								// may block, but exit with flag
								let transport = tarpc_bincode_transport::new(conn2);
								let client = await!(crate::worker::new_stub(
									tarpc::client::Config::default(),
									transport
								))
								.unwrap();

								await!(inner.workers.lock()).insert(session_id.clone(), client);

								info!(log, "[Broker] New worker client registered";"id"=>&session_id);
							}
							await!(inner.stop_flag.clone()).unwrap();
						},
					)
					.fuse();

					let mut stop_flag = inner.stop_flag.clone().fuse();
					futures::select! {
						_ = session_serve => {
							info!(log, "[Broker] Session closed"; "reason" => "rpc server TCP connection closed by peer","id"=>&session_id);
						},
						_ = fut2 => {
							info!(log, "[Broker] Session closed"; "reason" => "worker client TCP connection closed by peer","id"=>&session_id);
						},
						_ = stop_flag => {
							info!(log, "[Broker] Session closed"; "reason" => "STOP signal","id"=>&session_id);
						},
					};
					// clean-up
					await!(inner.workers.lock()).remove(&session_id);
				},
			);
		}
		info!(log, "[Broker] exit");
	}
}

pub struct BrokerInner {
	bind_addr: SocketAddr,
	workers: Mutex<HashMap<String, crate::worker::Client>>, // Mutex: Send
	// worker_conns: Mutex<HashMap<String,WorkerInfo>>,
	jobs: Mutex<BTreeMap<String, Job>>, // order matters. can be concurrently used by multiple worker rpc calls
	pending_job_ids: Mutex<BTreeSet<String>>, // order matters. can be concurrently used by multiple worker rpc calls
	stop_flag: crate::oneshot::OneshotFlag<()>,
}

impl BrokerInner {
	pub fn stop(&self) {}
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BrokerInfo {
	pub bind_addr: SocketAddr,
	pub jobs: Vec<Job>,
	pub workers: Vec<String>,
}

// instance per worker connection
#[derive(Clone)]
struct BrokerRPCServerImpl {
	// Se	nd
	broker: Arc<BrokerInner>,
}

impl Service for BrokerRPCServerImpl {
	type PingFut = Ready<()>;
	fn ping(self, _: context::Context) -> Self::PingFut {
		futures::future::ready(())
	}

	/// Broker <-> Worker
	type JobUpdateFut = Pin<Box<dyn Future<Output = ()> + Send>>;
	fn job_update(
		self,
		_: context::Context,
		job_id: String,
		status: JobStatus,
	) -> Self::JobUpdateFut {
		let log = crate::logger::get_logger();
		info!(log, "[Broker] job_update()");
		Box::pin(
			async move {
				await!(self.broker.jobs.lock())
					.get_mut(&job_id)
					.unwrap()
					.status = status;
			},
		)
	}

	type JobRequestFut = std::pin::Pin<Box<dyn Future<Output = Option<Job>> + Send>>;
	fn job_request(self, _: context::Context, capacity: WorkerCapacity) -> Self::JobRequestFut {
		let log = crate::logger::get_logger();
		info!(log, "[Broker] job_request()"; "capacity"=>?capacity);
		Box::pin(
			async move {
				let mut jobs = await!(self.broker.jobs.lock());
				let mut pending_job_ids = await!(self.broker.pending_job_ids.lock());

				match 'hunt: {
					for id in &*pending_job_ids {
						if let Some(allocation) = capacity.can_run_job(&jobs[id].spec.require) {
							break 'hunt Some((id.clone(), allocation));
						}
					}
					None
				} {
					Some((id, allocation)) => {
						pending_job_ids.remove(&id);
						let mut job = jobs.get_mut(&id).unwrap();
						job.allocation = Some(allocation);
						return Some(job.clone());
					}
					None => {
						// no available job
						return None;
					}
				};
			},
		)
	}

	/// Broker <-> CLI
	type JobEnqueueFut = Pin<Box<dyn Future<Output = ()> + Send>>;
	fn job_enqueue(self, _: context::Context, spec: JobSpecification) -> Self::JobEnqueueFut {
		let log = crate::logger::get_logger();
		info!(log, "[Broker] enqueue()"; "spec"=>?spec);
		Box::pin(
			async move {
				let job = spec.build();
				let mut jobs = await!(self.broker.jobs.lock());
				let mut pending_job_ids = await!(self.broker.pending_job_ids.lock());

				let job_id = job.id.clone();
				jobs.insert(job_id.clone(), job);
				pending_job_ids.insert(job_id);

				// broadcast to clients
				let mut workers = await!(self.broker.workers.lock());
				// let futs = vec![];
				info!(log, "Broadcasting to workers"; "num_workers"=>workers.len());
				for (_worker_id, worker_client) in workers.iter_mut() {
					await!(worker_client.on_new_job(context::current())).unwrap();
				}
			},
		)
	}

	type InfoFut = Pin<Box<dyn Future<Output = BrokerInfo> + Send>>;
	fn info(self, _: context::Context) -> Self::InfoFut {
		Box::pin(
			async move {
				let jobs = await!(self.broker.jobs.lock());

				BrokerInfo {
					bind_addr: self.broker.bind_addr.clone(),
					jobs: jobs.values().cloned().collect(),
					workers: Default::default(),
				}
			},
		)
	}
}

pub async fn new_broker_client(
	addr: SocketAddr,
) -> Result<Client, Box<dyn std::error::Error + 'static>> {
	let log = crate::logger::get_logger();
	info!(log, "[Client] Connecting to Broker."; "addr"=>&addr);
	let stream: TcpStream = await!(TcpStream::connect(&addr).compat())?;

	let mux = yamux::Connection::new(stream, yamux::Config::default(), yamux::Mode::Client);
	let stream = mux
		.open_stream()
		.expect("[Client] Failed to open 1st stream")
		.unwrap(); // client

	let transport = tarpc_bincode_transport::Transport::from(stream);
	let client = await!(new_stub(tarpc::client::Config::default(), transport))?;
	info!(log, "[Client] Connected to Broker.");
	Ok(client)
}
