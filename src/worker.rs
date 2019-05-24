use crate::objects::{Job, JobStatus, WorkerCapacity};
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
use tokio::timer::Interval;
use tokio_process::CommandExt;

use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::mpsc::UnboundedSender;
use futures::compat::Future01CompatExt;
use futures::lock::Mutex;
use futures::{future::Fuse, future::Ready, Future, FutureExt, StreamExt};

enum Message {
	JobUpdate { job_id: String, status: JobStatus },
	TrySchedule,
	SendQueueInfos,
}

pub struct WorkerConfig {
	pub broker_addr: SocketAddr,
	pub capacity: WorkerCapacity,
}

impl WorkerConfig {
	pub fn build(self, stop_flag: crate::oneshot::OneshotFlag<()>) -> Worker {
		let (sender, receiver) = futures::channel::mpsc::unbounded();
		let available = Mutex::new(self.capacity.clone());

		Worker {
			inner: Arc::new(WorkerInner {
				broker_addr: self.broker_addr,
				sender,
				queues: Default::default(),
				stop_flag,

				capacity: self.capacity,
				available,
			}),
			receiver: Mutex::new(receiver),
		}
	}
}

tarpc::service! {
	rpc status() -> String;
	rpc on_new_job();
	rpc info() -> WorkerInfo;
	// rpc info_jobs();
	// rpc info_resources();
}

pub struct Worker {
	/// reason for inner pattern:
	/// 	1. async requires 'static
	/// 	2. Worker tarpc server requires `self`.
	/// 	Hence only attributes required for tarpc server are in inner.
	///
	/// 	on_new_job() requires `sender`.
	pub inner: Arc<WorkerInner>,
	receiver: Mutex<UnboundedReceiver<Message>>,
}

impl Worker {
	pub async fn create_queue(&self, config: QueueConfig) {
		let mut queues = await!(self.inner.queues.lock());
		queues.insert(config.name.clone(), Arc::new(config.build()));
		self.inner
			.sender
			.unbounded_send(Message::SendQueueInfos)
			.expect("cannot enqueue worker msg");
	}
	pub async fn delete_queue(&self, name: String) {
		let mut queues = await!(self.inner.queues.lock());
		queues.remove(&name);
		self.inner
			.sender
			.unbounded_send(Message::SendQueueInfos)
			.expect("cannot enqueue worker msg");
	}
}

pub struct WorkerInner {
	broker_addr: SocketAddr,
	sender: UnboundedSender<Message>,
	queues: Mutex<BTreeMap<String, Arc<Queue>>>,
	stop_flag: crate::oneshot::OneshotFlag<()>,
	capacity: WorkerCapacity,
	available: Mutex<WorkerCapacity>,
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
							let mut available = await!(inner.available.lock());
							for (q_name, q) in queues.iter() {
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
										inner.clone().spawn_job(q, job, inner.sender.clone()),
									);
								}
							}
						}
						Message::SendQueueInfos => {
							// send queue info
							let queues = await!(inner.queues.lock());
							let q_infos = await!(QueueInfo::vec_from_queues(&queues));
							info!(log, "[Worker] msg: SendQueueInfos");
							await!(client.update_worker_state(context::current(), q_infos))
								.context("[Worker] fail update_worker_state()")?;
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

impl WorkerInner {
	async fn spawn_job(self: Arc<Self>, q: Arc<Queue>, job: Job, sender: UnboundedSender<Message>) {
		use std::process::ExitStatus;
		use std::process::Stdio;

		let job_id = job.id.clone();
		let sender_ = sender.clone();
		let job_ = job.clone();
		let cmd = q.cmd.clone();
		let envs = q.envs.clone();
		let cwd = q.cwd.clone();
		q.running
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		let result: Result<ExitStatus, failure::Error> = await!(
			async move {
				let sender = sender_;
				let mut job = job_;
				// TODO: lock
				job.status = JobStatus::Running { pid: 0 };
				let allocation = &job
					.allocation
					.as_ref()
					.ok_or(failure::err_msg("spawning job with no allocation"))?;

				let cmd = [&cmd[..], &job.spec.cmd[..]].concat();
				let envs = [&envs[..], &job.spec.envs[..]].concat();

				let child = std::process::Command::new(&cmd[0])
					.args(
						cmd[1..]
							.iter()
							.map(|x| -> Result<String, failure::Error> {
								Ok(strfmt::strfmt(x, &allocation.0)
									.context("cmd interpolation failed")?)
							})
							.collect::<Result<Vec<String>, failure::Error>>()?,
					)
					.envs(
						envs.iter()
							.map(|(k, v)| -> Result<_, failure::Error> {
								let a = strfmt::strfmt(k, &allocation.0)
									.context("env key interpolation failed")?;
								let b = strfmt::strfmt(v, &allocation.0)
									.context("env value interpolation failed")?;
								Ok((a, b))
							})
							.collect::<Result<Vec<_>, failure::Error>>()?,
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

		q.running
			.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

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

	type InfoFut = std::pin::Pin<Box<dyn Future<Output = WorkerInfo> + Send>>;
	fn info(self, _: context::Context) -> Self::InfoFut {
		Box::pin(async move { await!(WorkerInfo::from(self.worker.clone())) })
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueConfig {
	pub name: String,
	pub cwd: PathBuf,
	pub cmd: Vec<String>,
	pub envs: Vec<(String, String)>,
}

impl QueueConfig {
	pub fn build(self) -> Queue {
		Queue {
			name: self.name,
			cwd: self.cwd,
			cmd: self.cmd,
			envs: self.envs,
			jobs: Default::default(),
			running: Default::default(),
		}
	}
}

pub struct Queue {
	pub name: String,
	pub cwd: PathBuf,
	pub cmd: Vec<String>,
	pub envs: Vec<(String, String)>,
	jobs: Mutex<BTreeMap<String, Job>>,

	// stats
	pub running: std::sync::atomic::AtomicU32,
}

impl Queue {
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerNodeSpec {
	pub hostname: String,
	pub cpu_num: u32,
	pub os: String,
	pub os_release: String,
	pub runnning_since: chrono::DateTime<chrono::Utc>,
}

impl WorkerNodeSpec {
	fn new() -> WorkerNodeSpec {
		WorkerNodeSpec {
			hostname: sys_info::hostname().unwrap_or("[unknown]".to_string()),
			cpu_num: sys_info::cpu_num().unwrap_or(0),
			os: sys_info::os_type().unwrap_or("[unknown]".to_string()),
			os_release: sys_info::os_release().unwrap_or("[unknown]".to_string()),
			runnning_since: chrono::Utc::now(),
		}
	}

	pub fn get_uptime(&self) -> chrono::Duration {
		chrono::Utc::now().signed_duration_since(self.runnning_since)
	}
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueInfo {
	pub name: String,
	pub running: u32,
	// pending: usize,
}

impl QueueInfo {
	async fn from(q: &Queue) -> QueueInfo {
		QueueInfo {
			name: q.name.clone(),
			running: q.running.load(std::sync::atomic::Ordering::SeqCst),
		}
	}

	async fn vec_from_queues(queues: &BTreeMap<String, Arc<Queue>>) -> Vec<QueueInfo> {
		let mut q_infos = vec![];
		for (_q_name, q) in queues.iter() {
			q_infos.push(await!(QueueInfo::from(&q)));
		}
		q_infos
	}
}

/// has a detailed information about node, including jobs
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
	// static
	pub broker_addr: SocketAddr,
	pub node_spec: WorkerNodeSpec,
	// dynamic
	pub queue_infos: Vec<QueueInfo>,
	// pub state: WorkerState, // cpu load, running job num, ..

	pub available: WorkerCapacity,
}

impl WorkerInfo {
	pub async fn from(worker: Arc<WorkerInner>) -> WorkerInfo {
		let queues = await!(worker.queues.lock());
		let q_infos = await!(QueueInfo::vec_from_queues(&queues));

		let available = await!(worker.available.lock()).clone();

		WorkerInfo {
			broker_addr: worker.broker_addr.clone(),
			node_spec: WorkerNodeSpec::new(),
			queue_infos: q_infos,
			available,
		}
	}

	pub fn display_columns(&self) -> Vec<String> {
		vec![
			self.node_spec.hostname.clone(),
			format!("{} ({})", self.node_spec.os, self.node_spec.os_release),
			self
				.node_spec
				.get_uptime()
				.to_std()
				.map(|d| timeago::Formatter::new().convert(d))
				.unwrap_or("time sync mismatch".to_string()),
			self
				.queue_infos
				.iter() // queues
				.map(|q_info| format!(
					"{} ({}? running)",
					&q_info.name, &q_info.running
				))
				.collect::<Vec<_>>()
				.join(", ")
		]
	}
}
