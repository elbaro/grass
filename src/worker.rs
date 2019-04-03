use crate::objects::{Job, JobStatus, WorkerCapacity};
#[allow(unused_imports)]
use slog::{error, info};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio_process::CommandExt;

use tokio::net::TcpStream;
use tokio::prelude::*;

use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::mpsc::UnboundedSender;
use futures::compat::Future01CompatExt;
use futures::{future::Ready, StreamExt};
use futures01::future::Future;

enum Message {
	JobUpdate { job_id: String, status: JobStatus },
	TrySchedule,
}

pub struct WorkerConfig {
	pub broker_addr: SocketAddr,
	pub resources: WorkerCapacity,
}

impl WorkerConfig {
	pub fn build(self, is_dead: stream_cancel::Tripwire) -> Worker {
		let (sender, receiver) = futures::channel::mpsc::unbounded();
		let available = RwLock::new(self.resources.clone());
		Worker {
			inner: Arc::new(WorkerInner {
				broker_addr: self.broker_addr,
				resources: self.resources,
				available,
				sender,
				receiver,
				jobs: Default::default(),
				is_dead,
			}),
		}
	}
}

tarpc::service! {
	rpc status() -> String;
	rpc on_new_job();
}

pub struct Worker {
	inner: Arc<WorkerInner>,
}

pub struct WorkerInner {
	broker_addr: SocketAddr,

	resources: WorkerCapacity,
	available: RwLock<WorkerCapacity>,

	sender: UnboundedSender<Message>,
	receiver: UnboundedReceiver<Message>,

	jobs: BTreeMap<String, Job>,

	is_dead: stream_cancel::Tripwire,
}

impl Worker {
	pub fn stop(&self) {
		self.inner.stop();
	}
	pub async fn run_async(&self) -> Result<(), Box<dyn std::error::Error + 'static>> {
		let log = crate::logger::get_logger();

		info!(log, "[Worker] Connecting."; "broker_addr"=>&self.inner.broker_addr);
		let stream = await!(TcpStream::connect(&self.inner.broker_addr).compat()).unwrap();

		// let mux = yamux::Connection::new(stream, config, yamux::Mode::Client);
		// let conn1 = mux.open_stream().expect("Worker conn1 failed").unwrap(); // client
		// let conn2 = mux.open_stream().expect("Worker conn2 failed").unwrap(); // server

		// client
		let transport = tarpc_bincode_transport::new(stream);
		let client = await!(crate::broker::new_stub(
			tarpc::client::Config::default(),
			transport
		))
		.unwrap();

		// server
		// let transport = tarpc_bincode_transport::new(conn2); //.fuse(); // fuse from Future03 ext trait
		// let transport = transport.fuse();
		// let (sender, _recv) = futures::channel::mpsc::unbounded::<SocketAddr>();
		// let channel = tarpc::server::Channel::new_simple_channel(transport, sender);

		// crate::compat::tokio_spawn(channel.respond_with(serve(WorkerRPCServerImpl {
		// 	worker: self.inner.clone(),
		// 	client: Arc::new(client),
		// })));

		info!(log, "[Worker] Connected.");

		Ok(())
	}
}

impl WorkerInner {
	pub fn stop(&self) {}
	pub async fn run_async(&self) {
		// job1. message-processor
		// job2. respond to server events

		// loop {
		// 	let msg = self.receiver.recv();

		// 	match msg {
		// 		Ok(Message::JobUpdate { job_id, status }) => {
		// 			info!(log, "[Broker] JobUpdate"; "status"=>?status);

		// 			if let JobStatus::Finished { .. } = status {
		// 				// restore
		// 				let mut available = self.available.write().unwrap();
		// 				available.restore(&self.jobs[&job_id]);
		// 			}
		// 			client.job_update(job_id, status).unwrap();
		// 		}
		// 		Ok(Message::TrySchedule) => {
		// 			let mut available = self.available.write().unwrap();

		// 			match client.job_request(available.clone()).unwrap() {
		// 				Some(job) => {
		// 					available.consume(&job);
		// 					// spawn, consume
		// 					self.spawn_job(job).unwrap();
		// 				}
		// 				None => {}
		// 			}
		// 		}
		// 		Err(e) => {
		// 			error!(log, "channel disconnected"; "err"=>%e);
		// 			break;
		// 		}
		// 	};
		// }
	}
	async fn spawn_job(&self, mut job: Job) -> Result<(), Box<std::error::Error>> {
		use std::process::ExitStatus;
		use std::process::Stdio;

		// TODO: lock
		job.status = JobStatus::Running { pid: 0 };

		let sender = self.sender.clone();

		let allocation = &job.allocation.as_ref().unwrap();

		let status: Result<ExitStatus, String> = try {
			let child = std::process::Command::new(&job.spec.cmd[0])
				.args(
					job.spec.cmd[1..]
						.iter()
						.map(|x| strfmt::strfmt(x, &allocation.0).unwrap())
						.collect::<Vec<String>>(),
				)
				.envs(
					job.spec
						.envs
						.iter()
						.map(|(k, v)| {
							(
								strfmt::strfmt(k, &allocation.0).unwrap(),
								strfmt::strfmt(v, &allocation.0).unwrap(),
							)
						})
						.collect::<Vec<_>>(),
				)
				.current_dir(job.spec.cwd)
				.stdin(Stdio::null())
				.stdout(Stdio::null())
				.stderr(Stdio::null())
				.spawn_async()
				.map_err(|_err| "fail to spawn child")?;

			sender
				.unbounded_send(Message::JobUpdate {
					job_id: job.id.clone(),
					status: JobStatus::Running { pid: child.id() },
				})
				.map_err(|_err| "[Worker] error sending JobUpdate msg")?;

			// child is 01 future
			let status = await!(child
				.map_err(|err| format!("cannot wait child process: {}", err))
				.compat())
			.unwrap();
			status
		};

		let status = match status {
			Ok(status) => {
				if status.success() {
					Ok(())
				} else if let Some(code) = status.code() {
					Err(format!("exit code {}", code))
				} else {
					Err("exit by some signal".to_string())
				}
			}
			Err(err) => Err(format!("spawn error: {}", err)),
		};
		sender
			.unbounded_send(Message::JobUpdate {
				job_id: job.id,
				status: JobStatus::Finished {
					exit_status: status,
					duration: 0,
				},
			})
			.unwrap();
		Ok(())
	}
}

// instance per worker connection
#[derive(Clone)]
struct WorkerRPCServerImpl {
	worker: Arc<WorkerInner>,
	client: Arc<crate::broker::Client>,
}

use tarpc::context;
impl Service for WorkerRPCServerImpl {
	type OnNewJobFut = Ready<()>;
	fn on_new_job(self, _: context::Context) -> Self::OnNewJobFut {
		self.worker
			.sender
			.unbounded_send(Message::TrySchedule)
			.unwrap();
		futures::future::ready(())
	}

	type StatusFut = Ready<String>;
	fn status(self, _: context::Context) -> Self::StatusFut {
		futures::future::ready("asdf".to_string())
	}
}
