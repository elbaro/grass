use crate::objects::{Job, JobStatus, QueueCapacity};
#[allow(unused_imports)]
use slog::{error, info};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use failure::ResultExt;
use serde::{Deserialize, Serialize};
use tarpc::context;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio_process::CommandExt;

use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::mpsc::UnboundedSender;
use futures::compat::Future01CompatExt;
use futures::lock::Mutex;
use futures::{future::Fuse, future::Ready, Future, FutureExt, StreamExt};

enum Message {
	JobUpdate { job_id: String, status: JobStatus },
	TrySchedule,
}

pub struct WorkerConfig {
	pub broker_addr: SocketAddr,
}

impl WorkerConfig {
	pub fn build(self, stop_flag: crate::oneshot::OneshotFlag<()>) -> Worker {
		let (sender, receiver) = futures::channel::mpsc::unbounded();
		Worker {
			inner: Arc::new(WorkerInner {
				broker_addr: self.broker_addr,
				sender,
				queues: Default::default(),
				stop_flag,
			}),
			receiver: Mutex::new(receiver),
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

impl Worker {
	pub async fn create_queue(&self, config: QueueConfig) {
		let mut queues = await!(self.inner.queues.lock());
		queues.insert(config.name.clone(), Arc::new(config.build()));
	}
	pub async fn delete_queue(&self, name: String) {
		let mut queues = await!(self.inner.queues.lock());
		queues.remove(&name);
	}
}

pub struct WorkerInner {
	broker_addr: SocketAddr,
	sender: UnboundedSender<Message>,
	queues: Mutex<BTreeMap<String, Arc<Queue>>>,
	stop_flag: crate::oneshot::OneshotFlag<()>,
}

impl Worker {
	pub async fn run_async(&self) -> Result<(), failure::Error> {
		let log = slog_scope::logger();

		info!(log, "[Worker] Connecting"; "broker_addr"=>&self.inner.broker_addr);
		let stream = await!(TcpStream::connect(&self.inner.broker_addr).compat())
			.context("Couldn't connect to broker")?;

		let mux = yamux::Connection::new(stream, yamux::Config::default(), yamux::Mode::Client);

		// client
		let stream = mux
			.open_stream()
			.context("[Worker] cannot open mux")?
			.ok_or(failure::err_msg("[Worker] cannot open mux"))?; // client
		let transport = tarpc_bincode_transport::new(stream);
		let mut client = await!(crate::broker::new_stub(
			tarpc::client::Config::default(),
			transport
		))
		.context("cannot establish tarpc_bincode")?;
		await!(client.ping(context::current()))
			.context("test ping from worker to broker failed")?;

		// server
		let stream = mux
			.open_stream()
			.context("cannot open mux")?
			.ok_or(failure::err_msg("[Worker] cannot open mux"))?; // server
		let channel = {
			let transport = tarpc_bincode_transport::new(stream).fuse();
			let (sender, _recv) = futures::channel::mpsc::unbounded::<SocketAddr>();
			tarpc::server::Channel::new_simple_channel(transport, sender)
		};

		let mut serve = channel
			.respond_with(serve(WorkerRPCServerImpl {
				worker: self.inner.clone(),
				client: client.clone(),
			}))
			.fuse();

		let mut stop_flag = self.inner.stop_flag.clone().fuse();

		let inner = self.inner.clone();
		inner
			.sender
			.unbounded_send(Message::TrySchedule)
			.context("cannot enqueue worker msg q")?; // fetch jobs on start

		let mut receiver = await!(self.receiver.lock());
		let log_move = log.clone();
		let mut msg_process = Box::pin(
			async move {
				let log = log_move;
				while let Some(msg) = await!(receiver.next()) {
					match msg {
						Message::JobUpdate { job_id, status } => {
							info!(log, "[Worker] sending JobUpdate"; "job_id"=>&job_id, "status"=>?status);

							if let JobStatus::Finished { .. } = status {
								inner
									.sender
									.unbounded_send(Message::TrySchedule)
									.context("cannot enqueue msg")?;
							}
							await!(client.job_update(context::current(), job_id, status))
								.context("cannot call job_update()")?;
						}
						Message::TrySchedule => {
							info!(log, "[Worker] msg: TrySchedule; JobRequest");
							let queues = await!(inner.queues.lock());
							for (q_name, q) in queues.iter() {
								let mut available = await!(q.available.lock());

								if let Some(job) = await!(client.job_request(
									context::current(),
									q_name.to_string(),
									available.clone()
								))
								.expect("[Worker] broker_client.job_request)_ failed")
								{
									info!(log, "[Worker] received new job");
									// register job
									await!(q.jobs.lock()).insert(job.id.clone(), job.clone());

									available.consume(&job);
									// spawn, consume
									let q = q.clone();
									crate::compat::tokio_spawn(
										q.spawn_job(job, inner.sender.clone()),
									);
								}
							}
						}
					};
				}
				Result::<(), failure::Error>::Ok(())
			},
		)
		.fuse();

		let log = log.clone();
		futures::select! {
			_ = serve => {
				info!(log, "[Worker] Disconnect"; "reason"=>"TCP connection closed by peer");
			},
			result = msg_process => {
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

impl WorkerInner {}

// instance per worker connection
#[derive(Clone)]
struct WorkerRPCServerImpl {
	worker: Arc<WorkerInner>,
	client: crate::broker::Client, // Clone-able
}

impl Service for WorkerRPCServerImpl {
	type OnNewJobFut = Ready<()>;
	fn on_new_job(self, _: context::Context) -> Self::OnNewJobFut {
		let log = slog_scope::logger();
		info!(log, "[Worker] on_new_job()");
		self.worker
			.sender
			.unbounded_send(Message::TrySchedule)
			.unwrap();
		// possible that msg is processed before the end of this call
		// broker calls on_new_job() -> worker schedule msg -> worker calls job_request
		// concurrent call on_new_job() -> worker
		futures::future::ready(())
	}

	type StatusFut = Ready<String>;
	fn status(self, _: context::Context) -> Self::StatusFut {
		let log = slog_scope::logger();
		info!(log, "[Worker] status()");
		futures::future::ready("[dummy status]".to_string())
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueConfig {
	pub name: String,
	pub cwd: PathBuf,
	pub cmd: Vec<String>,
	pub envs: Vec<(String, String)>,
	pub capacity: QueueCapacity,
}

impl QueueConfig {
	pub fn build(self) -> Queue {
		let available = Mutex::new(self.capacity.clone());
		Queue {
			name: self.name,
			cwd: self.cwd,
			cmd: self.cmd,
			envs: self.envs,
			capacity: self.capacity,
			available,
			jobs: Default::default(),
		}
	}
}

pub struct Queue {
	pub name: String,
	pub cwd: PathBuf,
	pub cmd: Vec<String>,
	pub envs: Vec<(String, String)>,
	pub capacity: QueueCapacity,
	pub available: Mutex<QueueCapacity>,
	jobs: Mutex<BTreeMap<String, Job>>,
}

impl Queue {
	async fn spawn_job(self: Arc<Self>, job: Job, sender: UnboundedSender<Message>) {
		use std::process::ExitStatus;
		use std::process::Stdio;

		let job_id = job.id.clone();
		let sender_ = sender.clone();
		let job_ = job.clone();
		let cmd = self.cmd.clone();
		let envs = self.envs.clone();
		let cwd = self.cwd.clone();
		let result: Result<ExitStatus, failure::Error> = await!(
			async move {
				let sender = sender_;
				let mut job = job_;
				// TODO: lock
				job.status = JobStatus::Running { pid: 0 };
				let allocation = &job.allocation.as_ref().unwrap();

				let cmd = [&cmd[..], &job.spec.cmd[..]].concat();
				let envs = [&envs[..], &job.spec.envs[..]].concat();

				let child = std::process::Command::new(&cmd[0])
					.args(
						cmd[1..]
							.iter()
							.map(|x| strfmt::strfmt(x, &allocation.0).unwrap())
							.collect::<Vec<String>>(),
					)
					.envs(
						envs.iter()
							.map(|(k, v)| {
								(
									strfmt::strfmt(k, &allocation.0).unwrap(),
									strfmt::strfmt(v, &allocation.0).unwrap(),
								)
							})
							.collect::<Vec<_>>(),
					)
					.current_dir(cwd)
					.stdin(Stdio::null())
					.stdout(Stdio::null())
					.stderr(Stdio::null())
					.spawn_async()
					.context("fail to spawn child")?;

				sender
					.unbounded_send(Message::JobUpdate {
						job_id: job.id.clone(),
						status: JobStatus::Running { pid: child.id() },
					})
					.context("[Worker] error sending JobUpdate msg")?;

				let status = await!(child.compat()).context("cannot wait child process")?;
				Ok(status)
			}
		);

		let status = match result {
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

		// restore resource
		{
			let mut available = await!(self.available.lock());
			available.restore(&job);
		}

		sender
			.unbounded_send(Message::JobUpdate {
				job_id,
				status: JobStatus::Finished {
					exit_status: status,
					duration: 0,
				},
			})
			.unwrap();
	}
}
