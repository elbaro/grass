use crate::objects::{Job, JobStatus, WorkerCapacity};
#[allow(unused_imports)]
use slog::{error, info};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio_process::CommandExt;
use tokio::net::TcpStream;
use tokio::prelude::*;

use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::mpsc::UnboundedSender;
use futures::compat::Future01CompatExt;
use futures::lock::Mutex;
use futures::{future::Ready, FutureExt, StreamExt};
use futures01::future::Future;

use tarpc::context;

enum Message {
	JobUpdate { job_id: String, status: JobStatus },
	TrySchedule,
}

pub struct WorkerConfig {
	pub broker_addr: SocketAddr,
	pub resources: WorkerCapacity,
}

impl WorkerConfig {
	pub fn build(self, stop_flag: crate::oneshot::OneshotFlag<()>) -> Worker {
		let (sender, receiver) = futures::channel::mpsc::unbounded();
		let available = Mutex::new(self.resources.clone());
		Worker {
			inner: Arc::new(WorkerInner {
				broker_addr: self.broker_addr,
				resources: self.resources,
				available,
				sender,
				jobs: Default::default(),
				stop_flag,
			}),
			receiver:Mutex::new(receiver),
		}
	}
}

tarpc::service! {
	rpc status() -> String;
	rpc on_new_job();
}

pub struct Worker {
	/// reason for inner pattern:
	/// 	1. async requires 'static
	/// 	2. Worker tarpc server requires `self`.
	/// 	Hence only attributes required for tarpc server are in inner.
	///
	/// 	on_new_job() requires `sender`.
	inner: Arc<WorkerInner>,
	receiver: Mutex<UnboundedReceiver<Message>>,
}

pub struct WorkerInner {
	broker_addr: SocketAddr,
	resources: WorkerCapacity,
	available: Mutex<WorkerCapacity>,
	sender: UnboundedSender<Message>,
	jobs: BTreeMap<String, Job>,
	stop_flag: crate::oneshot::OneshotFlag<()>,
}

impl Worker {
	pub async fn run_async(&self) -> Result<(), Box<dyn std::error::Error + 'static>> {
		let log = crate::logger::get_logger();

		info!(log, "[Worker] Connecting."; "broker_addr"=>&self.inner.broker_addr);
		let stream = await!(TcpStream::connect(&self.inner.broker_addr).compat()).unwrap();

		let mux = yamux::Connection::new(stream, yamux::Config::default(), yamux::Mode::Client);

		// client
		let stream = mux.open_stream().expect("Worker failed").unwrap(); // client
		let transport = tarpc_bincode_transport::new(stream);
		let mut client = await!(crate::broker::new_stub(
			tarpc::client::Config::default(),
			transport
		))
		.unwrap();

		// server
		let stream = mux.open_stream().expect("Worker failed").unwrap(); // server
		let transport = tarpc_bincode_transport::new(stream); // fuse from Future03 ext trait
		let transport = transport.fuse();
		let (sender, _recv) = futures::channel::mpsc::unbounded::<SocketAddr>();
		let channel = tarpc::server::Channel::new_simple_channel(transport, sender);

		let mut serve = channel
			.respond_with(serve(WorkerRPCServerImpl {
				worker: self.inner.clone(),
				client: client.clone(),
			}))
			.fuse();

		let mut stop_flag = self.inner.stop_flag.clone().fuse();


		let inner = self.inner.clone();
		let mut receiver = await!(self.receiver.lock());
		let log_move = log.clone();
		let mut msg_process = Box::pin(async move {
			let log = log_move;
			while let Some(msg) = await!(receiver.next()) {
				match msg {
					Message::JobUpdate { job_id, status } => {
						info!(log, "[Broker] JobUpdate"; "status"=>?status);

						if let JobStatus::Finished { .. } = status {
							// restore
							let mut available = await!(inner.available.lock());
							available.restore(&inner.jobs[&job_id]);
						}
						await!(client.job_update(context::current(), job_id, status)).unwrap();
					}
					Message::TrySchedule => {
						let mut available = await!(inner.available.lock());

						match await!(client.job_request(context::current(), available.clone())).unwrap() {
							Some(job) => {
								available.consume(&job);
								// spawn, consume
								await!(inner.spawn_job(job)).unwrap();
							}
							None => {}
						}
					}
				};
			}
		}).fuse();

		let log = log.clone();
		futures::select! {
			_ = serve => {
				info!(log, "[Worker] Disconnect"; "reason"=>"TCP connection closed by peer");
			},
			_ = msg_process => {
				info!(log, "[Worker] Disconnect"; "reason"=>"some error while processing msg");
			}
			_ = stop_flag => {
				info!(log, "[Worker] Disconnect"; "reason"=>"STOP signal");
			},
		};

		info!(log, "[Worker] exit");

		Ok(())
	}
}

impl WorkerInner {

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
	client: crate::broker::Client, // Clone-able
}

impl Service for WorkerRPCServerImpl {
	type OnNewJobFut = Ready<()>;
	fn on_new_job(self, _: context::Context) -> Self::OnNewJobFut {
		println!(">>>>>>>>>>>>>     on new job");
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
