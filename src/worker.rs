use crate::objects::{Job, JobStatus, WorkerCapacity};
#[allow(unused_imports)]
use slog::{error, info};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::RwLock;
use tokio_process::CommandExt;

use futures::compat::Future01CompatExt;
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
	pub fn build(self) -> Worker {
		let (sender, receiver) = futures::channel::mpsc::unbounded();
		let available = RwLock::new(self.resources.clone());
		Worker {
			broker_addr: self.broker_addr,
			resources: self.resources,
			available,
			sender,
			receiver,
			jobs: Default::default(),
		}
	}
}

use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::mpsc::UnboundedSender;

pub struct Worker {
	broker_addr: SocketAddr,

	resources: WorkerCapacity,
	available: RwLock<WorkerCapacity>,

	sender: UnboundedSender<Message>,
	receiver: UnboundedReceiver<Message>,

	jobs: BTreeMap<String, Job>,
}

impl Worker {
	pub fn stop(&self) {}
	pub async fn run_async(&self) {
		// let stream = TcpStream::connect(self.broker_addr).unwrap();
		// let mut client =
		// 	crate::rpc::BrokerRPCClient::new(essrpc::transports::BincodeTransport::new(stream));
		// let log = crate::logger::get_logger();
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
