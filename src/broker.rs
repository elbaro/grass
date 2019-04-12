use crate::objects::{Job, JobSpecification, JobStatus, QueueCapacity};
use crate::worker::WorkerInfo;

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
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

use failure::ResultExt;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use slog::{error, info, warn};
use tarpc::context;
use tarpc::server::Handler;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

pub struct BrokerConfig {
	pub bind_addr: SocketAddr,
	pub cert: Option<PathBuf>,
	pub cert_pass: Option<String>,
}

impl BrokerConfig {
	pub fn build(self, stop_flag: crate::oneshot::OneshotFlag) -> Broker {
		Broker {
			inner: Arc::new(BrokerInner {
				bind_addr: self.bind_addr,
				workers: Default::default(),
				worker_conns: Default::default(),
				jobs: Default::default(),
				pending_job_ids: Default::default(),
				stop_flag,
			}),
			cert: self.cert,
			cert_pass: self.cert_pass,
		}
	}
}

tarpc::service! {
	rpc ping();
	rpc job_update(job_id: String, status: JobStatus);
	rpc job_request(q_name: String, capacity: QueueCapacity) -> Option<Job>;
	rpc job_enqueue(spec: JobSpecification);
	// rpc job_enqueue(spec: String);
	rpc info() -> BrokerInfo;
}

pub struct Broker {
	// reason for inner pattern
	pub inner: Arc<BrokerInner>,
	pub cert: Option<PathBuf>,
	pub cert_pass: Option<String>,
}

impl Broker {
	pub fn stop(&self) {
		self.inner.stop();
	}
	pub async fn run_async(&self) -> Result<(), failure::Error> {
		let log = slog_scope::logger();
		info!(log, "[Broker] Listening."; "bind"=>%self.inner.bind_addr);

		let listener = TcpListener::bind(&self.inner.bind_addr).context("cannot bind to socket")?;

		// let mut serving = listener.incoming().compat();
		use crate::oneshot::StreamExt as OneshotStreamExt;
		let mut serving = listener
			.incoming()
			.compat()
			.take_until(self.inner.stop_flag.clone().map(|_| ()));

		let inner = self.inner.clone();
		while let Some(stream) = await!(serving.next()) {
			clone_all::clone_all!(log, inner);
			crate::compat::tokio_try_spawn(
				async move {
					// let inner = (&inner).clone();
					let session_id = uuid::Uuid::new_v4().to_hyphenated().to_string();
					info!(log, "[Broker] New session";"id"=>&session_id);
					let stream = stream.context("cannot open stream")?;

					// yamux
					let mut mux = yamux::Connection::new(
						stream,
						yamux::Config::default(),
						yamux::Mode::Server,
					);
					let stream = await!(mux.next())
						.ok_or(failure::err_msg("cannot open mux"))?
						.context("cannot open mux")?;

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
								let mut client = await!(crate::worker::new_stub(
									tarpc::client::Config::default(),
									transport
								))
								.context("error from tarpc serving")?;

								// request initial WorkerInfo
								let info = await!(client.info(context::current()))
									.expect("fail to info()");
								await!(inner.workers.lock()).insert(session_id.clone(), info);
								await!(inner.worker_conns.lock())
									.insert(session_id.clone(), client);

								info!(log, "[Broker] New worker client registered";"id"=>&session_id);
							}
							let _ = await!(inner.stop_flag.clone()); // cancel
							Result::<(), failure::Error>::Ok(())
						},
					)
					.fuse();

					let mut stop_flag = inner.stop_flag.clone().fuse();
					futures::select! {
						_ = session_serve => {
							info!(log, "[Broker] Session closed"; "reason" => "rpc server TCP connection closed by peer","id"=>&session_id);
						},
						result = fut2 => {
							info!(log, "[Broker] Session closed"; "reason" => "worker client TCP connection closed by peer","id"=>&session_id);
						},
						_ = stop_flag => {
							info!(log, "[Broker] Session closed"; "reason" => "STOP signal","id"=>&session_id);
						},
					};
					// clean-up
					await!(inner.workers.lock()).remove(&session_id);
					await!(inner.worker_conns.lock()).remove(&session_id);
					Ok(())
				},
			);
		}
		info!(log, "[Broker] exit");
		Ok(())
	}
}

pub struct BrokerInner {
	bind_addr: SocketAddr,
	worker_conns: Mutex<HashMap<String, crate::worker::Client>>, // Mutex: Send
	workers: Mutex<HashMap<String, WorkerInfo>>,
	jobs: Mutex<IndexMap<String, Job>>, // order matters. can be concurrently used by multiple worker rpc calls
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
	pub workers: Vec<WorkerInfo>,
}

impl BrokerInfo {
	pub async fn from(broker: Arc<BrokerInner>) -> BrokerInfo {
		let jobs = await!(broker.jobs.lock());
		let workers = await!(broker.workers.lock());

		BrokerInfo {
			bind_addr: broker.bind_addr,
			jobs: jobs.values().cloned().collect(),
			workers: workers.values().cloned().collect(),
		}
	}
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
		let log = slog_scope::logger();
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
	fn job_request(
		self,
		_: context::Context,
		q_name: String,
		capacity: QueueCapacity,
	) -> Self::JobRequestFut {
		let log = slog_scope::logger();
		info!(log, "[Broker] job_request()"; "q"=>&q_name, "capacity"=>?capacity);
		Box::pin(
			async move {
				let mut jobs = await!(self.broker.jobs.lock());
				let mut pending_job_ids = await!(self.broker.pending_job_ids.lock());

				match 'hunt: {
					for id in &*pending_job_ids {
						if jobs[id].spec.q_name == q_name {
							if let Some(allocation) = capacity.can_run_job(&jobs[id].spec.require) {
								break 'hunt Some((id.clone(), allocation));
							}
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
	type JobEnqueueFut = Ready<()>;
	fn job_enqueue(self, _: context::Context, spec: JobSpecification) -> Self::JobEnqueueFut {
		let log = slog_scope::logger();
		info!(log, "[Broker] enqueue()"; "spec"=>?spec);
		crate::compat::tokio_try_spawn(Box::pin(
			async move {
				let job = spec.build();
				let mut jobs = await!(self.broker.jobs.lock());
				let mut pending_job_ids = await!(self.broker.pending_job_ids.lock());

				let job_id = job.id.clone();
				jobs.insert(job_id.clone(), job);
				pending_job_ids.insert(job_id);

				// broadcast to clients
				let mut worker_conns = await!(self.broker.worker_conns.lock());
				// let futs = vec![];
				info!(log, "Broadcasting to workers"; "num_workers"=>worker_conns.len());
				for (_worker_id, worker_client) in worker_conns.iter_mut() {
					await!(worker_client.on_new_job(context::current()))
						.context("fail to call on_new_job()")?;
				}
				Ok(())
			},
		));
		futures::future::ready(())
	}

	type InfoFut = Pin<Box<dyn Future<Output = BrokerInfo> + Send>>;
	fn info(self, _: context::Context) -> Self::InfoFut {
		Box::pin(async move { await!(BrokerInfo::from(self.broker)) })
	}
}

pub async fn new_broker_client(addr: SocketAddr) -> Result<Client, failure::Error> {
	let log = slog_scope::logger();
	info!(log, "[Client] Connecting to Broker."; "addr"=>&addr);
	let stream: TcpStream = await!(TcpStream::connect(&addr).compat())?;

	let mux = yamux::Connection::new(stream, yamux::Config::default(), yamux::Mode::Client);
	let stream = mux
		.open_stream()
		.context("[Client] Failed to open mux")?
		.ok_or(failure::err_msg("[Client] Failed to open mux"))?; // client

	let transport = tarpc_bincode_transport::Transport::from(stream);
	let client = await!(new_stub(tarpc::client::Config::default(), transport))?;
	info!(log, "[Client] Connected to Broker.");
	Ok(client)
}
