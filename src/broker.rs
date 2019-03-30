use crate::objects::{Job, JobSpecification, JobStatus, WorkerCapacity, WorkerInfo};
use essrpc::RPCServer;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::RwLock;

use futures::future::Future;

use essrpc::transports::BincodeTransport;
use tokio::net::TcpStream;
#[allow(unused_imports)]
use slog::{info,warn,error};

pub struct BrokerConfig {
	pub bind_addr: SocketAddr,
}

impl BrokerConfig {
	pub fn build(self) -> Broker {
		Broker {
			bind_addr: self.bind_addr,
			workers: Default::default(),
			jobs: Default::default(),
			pending_job_ids: Default::default(),
		}
	}
}

pub struct Broker {
	bind_addr: SocketAddr,
	workers: RwLock<HashMap<String, WorkerInfo>>,
	// worker_conns: RwLock<HashMap<String,WorkerInfo>>,
	jobs: RwLock<BTreeMap<String, Job>>, // order matters. can be concurrently used by multiple worker rpc calls
	pending_job_ids: RwLock<BTreeSet<String>>, // order matters. can be concurrently used by multiple worker rpc calls
}

impl Broker {
	pub fn stop(&self) {

	}
	pub fn run_sync(&self) {
		// let listener = tokio::net::UnixListener::bind("/tmp/broker.sock").unwrap();
		let log = crate::logger::get_logger();
		let listener = std::net::TcpListener::bind(&self.bind_addr).unwrap();
		crossbeam::scope(move |scope| {
			for stream in listener.incoming() {
				match stream {
					Ok(stream) => {
						info!(log, "new worker coming");
						let mut s = crate::rpc::BrokerRPCServer::new(
							BrokerRPCServerImpl { broker: &self },
							BincodeTransport::new(stream),
						);
						let log = log.clone();
						scope.spawn(move |_| {
							s.serve().unwrap();
							info!(log, "worker left")
						});
					}
					Err(err) => {
						break;
					}
				}
			}
		})
		.unwrap();
	}
}

// instance per worker connection
struct BrokerRPCServerImpl<'a> {
	broker: &'a Broker,
}

impl<'a> crate::rpc::Broker for BrokerRPCServerImpl<'a> {
	fn job_update(&self, job_id: String, status: JobStatus) -> Result<String, essrpc::RPCError> {
		self.broker
			.jobs
			.write()
			.unwrap()
			.get_mut(&job_id)
			.unwrap()
			.status = status;
		Ok("update".into())
	}
	fn job_request(&self, capacity: WorkerCapacity) -> Result<Option<Job>, essrpc::RPCError> {
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
				return Ok(Some(job.clone()));
			}
			None => {
				// no available job
				return Ok(None);
			}
		};
	}
	fn worker_introduce(&self) -> Result<String, essrpc::RPCError> {
		println!("     new worker");
		Ok("intro".into())
	}
	fn worker_heartbeat(&self) -> Result<String, essrpc::RPCError> {
		println!("     worker heartbeat");
		Ok("heartbeat".into())
	}

	/// Broker <-> CLI
	fn job_enqueue(&self, spec: JobSpecification) -> Result<(), essrpc::RPCError> {
		let job = spec.build();

		let mut jobs = self.broker.jobs.write().unwrap();
		let mut pending_job_ids = self.broker.pending_job_ids.write().unwrap();

		let job_id = job.id.clone();
		jobs.insert(job_id.clone(), job);
		pending_job_ids.insert(job_id);

		// broadcast to clients
		// for client in &self.clients {
		// client.notify_new_job();
		// }

		Ok(())
	}
	fn show(&self) -> Result<String, essrpc::RPCError> {
		Ok("implementing".to_string())
	}
}

use crate::rpc::BrokerRPCClient;
use essrpc::RPCClient;
pub fn new_broker_client(
	addr: SocketAddr,
) -> Result<BrokerRPCClient<BincodeTransport<TcpStream>>, Box<std::error::Error>> {
	let mut rt = tokio::runtime::Runtime::new().unwrap();
	let stream = rt.block_on(TcpStream::connect(&addr))?;
	Ok(BrokerRPCClient::new(BincodeTransport::new(stream)))
}
