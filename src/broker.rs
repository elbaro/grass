use crate::objects::{Job, JobSpecification, JobStatus, WorkerCapacity, WorkerInfo};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use futures::compat::Executor01CompatExt;
use futures::compat::Future01CompatExt;
use futures::compat::Stream01CompatExt;
use futures::{future::Ready, Future, FutureExt, Stream, StreamExt, TryFutureExt};
use futures01::Future as Future01;

use tarpc::context;
use tarpc::server::Handler;

use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::Future as TokioFuture;
use tokio::prelude::Stream as TokioStream;

#[allow(unused_imports)]
use slog::{error, info, warn};

pub struct BrokerConfig {
	pub bind_addr: SocketAddr,
}

impl BrokerConfig {
	pub fn build(self) -> Broker {
		Broker {
			inner: Arc::new(BrokerInner {
				bind_addr: self.bind_addr,
				workers: Default::default(),
				jobs: Default::default(),
				pending_job_ids: Default::default(),
			}),
		}
	}
}

tarpc::service! {
	rpc job_update(job_id: String, status: JobStatus);
	rpc job_request(capacity: WorkerCapacity) -> Option<Job>;
	rpc job_enqueue(spec: JobSpecification);
}

pub struct Broker {
	inner: Arc<BrokerInner>,
}
impl Broker {
	pub fn stop(&self) {
		self.inner.stop();
	}
	pub async fn run_async(&self) {
		let listener = TcpListener::bind(&self.inner.bind_addr).unwrap();
		let log = crate::logger::get_logger();

		// let serving = tarpc::server::Server::default().incoming(transport).respond_with(serve(BrokerRPCServerImpl));
		tarpc::init(tokio::executor::DefaultExecutor::current().compat());
		let inner = self.inner.clone();
		let serving = listener
			.incoming()
			.compat() // 0.1->0.3
			.for_each(move |stream| {
				// stream of Result<TcpStream,Error>
				let stream = stream.unwrap();

				info!(log, "[Broker] new worker session");
				let transport = tarpc_bincode_transport::new(stream).fuse(); // fuse from Future03 ext trait
				let (sender, _recv) = futures::channel::mpsc::unbounded::<SocketAddr>();
				let channel = tarpc::server::Channel::new_simple_channel(transport, sender);
				channel.respond_with(serve(BrokerRPCServerImpl {
					broker: inner.clone(),
				})) // 0.3
			}); // 0.3 Future

		await!(serving);
	}
}

pub struct BrokerInner {
	bind_addr: SocketAddr,
	workers: RwLock<HashMap<String, WorkerInfo>>,
	// worker_conns: RwLock<HashMap<String,WorkerInfo>>,
	jobs: RwLock<BTreeMap<String, Job>>, // order matters. can be concurrently used by multiple worker rpc calls
	pending_job_ids: RwLock<BTreeSet<String>>, // order matters. can be concurrently used by multiple worker rpc calls
}

impl BrokerInner {
	pub fn stop(&self) {}
}

// instance per worker connection
#[derive(Clone)]
struct BrokerRPCServerImpl {
	broker: Arc<BrokerInner>,
}

impl Service for BrokerRPCServerImpl {
	/// Broker <-> Worker
	type JobUpdateFut = Ready<()>;
	type JobRequestFut = Ready<Option<Job>>;
	// type WorkerIntroduceFut = Ready<()>;
	// type WorkerHeartbeatFut = Ready<()>;

	/// Broker <-> CLI
	type JobEnqueueFut = Ready<()>;
	// type ShowFut = Ready<String>;

	fn job_update(
		self,
		_: context::Context,
		job_id: String,
		status: JobStatus,
	) -> Self::JobUpdateFut {
		self.broker
			.jobs
			.write()
			.unwrap()
			.get_mut(&job_id)
			.unwrap()
			.status = status;
		futures::future::ready(())
	}
	fn job_request(self, _: context::Context, capacity: WorkerCapacity) -> Self::JobRequestFut {
		println!("    worker looking for a job:");

		let mut jobs = self.broker.jobs.write().unwrap();
		let mut pending_job_ids = self.broker.pending_job_ids.write().unwrap();

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
				return futures::future::ready(Some(job.clone()));
			}
			None => {
				// no available job
				return futures::future::ready(None);
			}
		};
	}
	// fn worker_introduce(self) -> Result<String, essrpc::RPCError> {
	// 	println!("     new worker");
	// 	Ok("intro".into())
	// }
	// fn worker_heartbeat(self) -> Result<String, essrpc::RPCError> {
	// 	println!("     worker heartbeat");
	// 	Ok("heartbeat".into())
	// }

	/// Broker <-> CLI
	fn job_enqueue(self, _: context::Context, spec: JobSpecification) -> Self::JobEnqueueFut {
		let job = spec.build();

		let mut jobs = self.broker.jobs.write().unwrap();
		let mut pending_job_ids = self.broker.pending_job_ids.write().unwrap();

		let job_id = job.id.clone();
		jobs.insert(job_id.clone(), job);
		pending_job_ids.insert(job_id);

		// broadcast to clients
		// for client in self.clients {
		// client.notify_new_job();
		// }

		futures::future::ready(())
	}
	// fn show(&self) -> ShowFut {
	// 	Ok("implementing".to_string())
	// }
}

pub async fn new_broker_client(
	addr: SocketAddr,
) -> Result<Client, Box<std::error::Error + 'static>> {
	let tcp: TcpStream = await!(TcpStream::connect(&addr).compat())?;
	// let transport = tarpc_bincode_transport::new(tcp);
	let transport = tarpc_bincode_transport::Transport::from(tcp);
	let client = await!(new_stub(tarpc::client::Config::default(), transport))?;
	Ok(client)
}
