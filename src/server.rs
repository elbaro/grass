use actix_web::{http::Method, HttpMessage, HttpRequest, HttpResponse, Responder};
use clone_all::clone_all;
use crossbeam::{
	atomic::AtomicCell,
	channel::{unbounded, Sender},
	thread,
};
use futures::future::Future;
use futures::stream::Stream;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use slog::{error, info, warn};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::ExitStatus;
use std::process::Stdio;
use std::sync::{Arc, RwLock};
use tokio_uds::UnixListener;

use crate::objects::{JobStatus, ResourceTypeCapacity};

// we have a global channel for all queues
pub enum Message {
	TrySchedule(String), // queue name
	JobRunning(JobRunningPayload),
	JobFinished(JobFinishedPayload),
}

pub struct JobRunningPayload {
	q_name: String,
	job_id: String,
	pid: u32,
}

pub struct JobFinishedPayload {
	q_name: String,
	job_id: String,
	status: Result<(), String>,
	duration: i32,
}

/// Job has information about running context.
/// Job consumes resources from a queue, and returns back when finished.
///
#[derive(Clone, Serialize, Deserialize)]
pub struct Job {
	pub id: String,
	pub cwd: PathBuf,
	pub cmd: Vec<String>,
	pub envs: Vec<(String, String)>,
	require: HashMap<String, Decimal>,
	pub status: JobStatus,
	pub allocation: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize)]
struct JobSpecification {
	cwd: PathBuf,
	cmd: Vec<String>,
	envs: Vec<(String, String)>,
	require: HashMap<String, Decimal>,
}

impl JobSpecification {
	fn build(self) -> Job {
		let id = uuid::Uuid::new_v4().to_hyphenated().to_string();
		Job {
			id,
			cwd: self.cwd,
			cmd: self.cmd,
			envs: self.envs,
			require: self.require,
			status: JobStatus::Created,
			allocation: None,
		}
	}
}

impl Job {
	/// requires: cwd, cmd
	fn spawn(
		&mut self,
		track: Option<(Sender<Message>, String)>,
	) -> Result<(), Box<std::error::Error>> {
		self.status = JobStatus::Running { pid: 0 };
		let job: Job = self.clone();
		std::thread::spawn(move || -> Result<(), Box<std::error::Error + Send>> {
			let allocation = job.allocation.as_ref().unwrap();
			let (sender, q_name) = track.unwrap();
			let status: Result<ExitStatus, String> = try {
				let mut child = std::process::Command::new(&job.cmd[0])
					.args(
						job.cmd[1..]
							.iter()
							.map(|x| strfmt::strfmt(x, allocation).unwrap())
							.collect::<Vec<String>>(),
					)
					.envs(
						job.envs
							.iter()
							.map(|(k, v)| {
								(
									strfmt::strfmt(k, allocation).unwrap(),
									strfmt::strfmt(v, allocation).unwrap(),
								)
							})
							.collect::<Vec<_>>(),
					)
					.current_dir(job.cwd)
					.stdin(Stdio::null())
					.stdout(Stdio::null())
					.stderr(Stdio::null())
					.spawn()
					.map_err(|err| format!("spawn error: {}", err))?;

				let _ = sender.send(Message::JobRunning(JobRunningPayload {
					q_name: q_name.clone(),
					job_id: job.id.clone(),
					pid: child.id(),
				}));

				let status = child
					.wait()
					.map_err(|err| format!("cannot wait child process: {}", err))?;

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
			let _ = sender.send(Message::JobFinished(JobFinishedPayload {
				q_name,
				job_id: job.id,
				status,
				duration: 0,
			}));

			Ok(())
		});

		Ok(())
	}
}

#[derive(Serialize, Deserialize)]
pub struct Queue {
	name: String,
	id: String,
	resources: HashMap<String, ResourceTypeCapacity>,
	available: HashMap<String, ResourceTypeCapacity>,
	pub future_jobs: VecDeque<Job>,
	pub running_jobs: HashMap<String, Job>,
	pub past_jobs: Vec<Job>,
}

#[derive(Serialize, Deserialize)]
struct QueueSpecification {
	name: String,
	resources: HashMap<String, ResourceTypeCapacity>,
}

impl QueueSpecification {
	fn build(self) -> Queue {
		let id = uuid::Uuid::new_v4().to_hyphenated().to_string();
		Queue {
			id,
			name: self.name,
			resources: self.resources.clone(),
			available: self.resources,
			future_jobs: VecDeque::new(),
			running_jobs: HashMap::new(),
			past_jobs: Vec::new(),
		}
	}
}

impl Queue {
	fn check_availability(&self, job: &Job) -> Option<HashMap<String, String>> {
		let mut allocation = HashMap::<String, String>::new();
		let chk = job.require.iter().all(|(res_type, amount)| {
			if self.available.contains_key(res_type) {
				// hdd1, hdd2, ..
				for (res_instance, res_capacity) in &self.available[res_type].0 {
					if res_capacity >= amount {
						allocation.insert(res_type.to_string(), res_instance.to_string()); // HDD: HDD1
						return true;
					}
				}
				false
			} else {
				// job fail
				// error!(log, "job requested non-existent resource"; "resource_type"=>res_type);
				false
			}
		});
		if chk {
			Some(allocation)
		} else {
			None
		}
	}

	fn run(&mut self, mut job: Job, sender: Sender<Message>) {
		// decrease availability
		for (res_type, req) in &job.require {
			*self
				.available
				.get_mut(res_type)
				.unwrap()
				.0
				.get_mut(&job.allocation.as_ref().unwrap()[res_type])
				.unwrap() -= *req;
		}
		job.spawn(Some((sender, self.name.clone()))).unwrap();
		self.running_jobs.insert(job.id.to_string(), job);
	}

	fn try_schedule(&mut self, sender: Sender<Message>) {
		if !self.future_jobs.is_empty() {
			if let Some(allocation) = self.check_availability(&self.future_jobs[0]) {
				let mut job = self.future_jobs.pop_front().unwrap();
				job.allocation = Some(allocation);
				self.run(job, sender);
			} else {
				eprintln!("job still pending");
				// error!(log, "job still pending");
			}
		}
	}
}

#[derive(Serialize, Deserialize)]
pub struct State {
	queues: HashMap<String, Queue>,
}
impl State {
	fn new() -> State {
		State {
			queues: HashMap::new(),
		}
	}
}

#[derive(Clone)]
pub struct AppState {
	log: slog::Logger,
	state: Arc<RwLock<State>>,
	is_exit: Arc<AtomicCell<bool>>,
	sender: Sender<Message>,
}
impl AppState {
	pub fn new(
		log: slog::Logger,
		state: Arc<RwLock<State>>,
		is_exit: Arc<AtomicCell<bool>>,
		sender: Sender<Message>,
	) -> AppState {
		info!(log, "--- AppState is created ---");
		AppState {
			log,
			state,
			is_exit,
			sender,
		}
	}
}

impl Drop for AppState {
	fn drop(&mut self) {
		info!(self.log, "AppState dropped"; "is_exit"=>self.is_exit.load());
	}
}

fn create_queue(
	r: HttpRequest<AppState>,
) -> impl Future<Item = HttpResponse, Error = actix_web::Error> {
	info!(r.state().log, "create-queue");
	let state = r.state().state.clone();
	r.payload()
		.concat2()
		.from_err::<actix_web::Error>()
		.and_then(move |body| {
			let q: Result<Queue, Box<std::error::Error>> = try {
				let s = std::str::from_utf8(&body)?;
				serde_json::from_str::<QueueSpecification>(s)?.build()
			};
			match q {
				Ok(q) => {
					let mut state = state.write().unwrap();
					if state.queues.contains_key(&q.name) {
						Ok(HttpResponse::from("q already exists"))
					} else {
						state.queues.insert(q.name.clone(), q);
						Ok(HttpResponse::from("created"))
					}
				}
				Err(e) => {
					error!(r.state().log, "error parsing json"; "err"=>%e);
					Ok(HttpResponse::from("error parsing json"))
				}
			}
		})
}
fn delete_queue(
	r: HttpRequest<AppState>,
) -> impl Future<Item = HttpResponse, Error = actix_web::Error> {
	let log = r.state().log.clone();
	info!(log, "delete_queue");

	let state = r.state().state.clone();
	r.payload()
		.concat2()
		.from_err::<actix_web::Error>()
		.and_then(move |body| {
			let value: Value = serde_json::from_str(std::str::from_utf8(&body).unwrap()).unwrap();
			if let Some(name) = value["name"].as_str() {
				let mut state = state.write().unwrap();
				if state.queues.contains_key(name) {
					info!(log, "delete_queue - removed queue");
					state.queues.remove(name);
					Ok(HttpResponse::from("deleted"))
				} else {
					info!(log, "delete_queue - cannot find queue");
					Ok(HttpResponse::from("cannot find q"))
				}
			} else {
				info!(log, "delete_queue - missing fields");
				Ok(HttpResponse::from("missing name field"))
			}
		})
}
fn enqueue(r: HttpRequest<AppState>) -> impl Future<Item = HttpResponse, Error = actix_web::Error> {
	let state = r.state().state.clone();
	let sender = r.state().sender.clone();
	r.payload()
		.concat2()
		.from_err::<actix_web::Error>()
		.and_then(move |body| {
			let value: Value = serde_json::from_str(std::str::from_utf8(&body).unwrap()).unwrap();
			if let Some(queue) = value["queue"].as_str() {
				let mut state = state.write().unwrap();
				if state.queues.contains_key(queue) {
					let q = state.queues.get_mut(queue).unwrap();
					let job = serde_json::from_value::<JobSpecification>(value.clone())
						.unwrap()
						.build();
					q.future_jobs.push_back(job);
					sender.send(Message::TrySchedule(q.name.clone())).unwrap();
					Ok(HttpResponse::from("enqueued"))
				} else {
					Ok(HttpResponse::from("the queue does not exist"))
				}
			} else {
				Ok(HttpResponse::from("missing queue field"))
			}
		})
}
fn show(r: HttpRequest<AppState>) -> impl Future<Item = HttpResponse, Error = actix_web::Error> {
	info!(r.state().log, "show");
	let state = r.state().state.clone();
	r.payload()
		.concat2()
		.from_err::<actix_web::Error>()
		.and_then(move |body| {
			let value: Result<Value, Box<std::error::Error>> =
				try { serde_json::from_str(std::str::from_utf8(&body)?)? };
			match value {
				Ok(value) => {
					let state = state.read().unwrap();
					if let Some(q_name) = value.get("queue") {
						if let Some(q_name) = q_name.as_str() {
							if let Some(q) = state.queues.get(q_name) {
								Ok(HttpResponse::Ok()
									.content_type("application/json")
									.body(
										serde_json::to_string(&q).unwrap()
										))
							} else {
								Ok(HttpResponse::from("cannot find q"))
							}
						} else {
							Ok(HttpResponse::from("q name is not string"))
						}
					} else {
						Ok(HttpResponse::Ok()
							.content_type("application/json")
							.body(serde_json::to_string(&state.queues).unwrap()))
					}
				}
				Err(e) => {
					error!(r.state().log, "error parsing json"; "err"=>%e, "body"=>std::str::from_utf8(&body).unwrap());
					Ok(HttpResponse::from("error parsing json"))
				}
			}
		})
}
fn exit(r: &HttpRequest<AppState>) -> impl Responder {
	info!(r.state().log, "received STOP signal");
	actix::System::current().stop();
	r.state().is_exit.store(true);
	"bye" // AppState drop -> sender drop -> channel disconnect
}

pub fn run(log: slog::Logger) {
	let listener = UnixListener::bind("/tmp/grass.sock").expect("bind failed");
	let sys = actix::System::new("unix-socket");
	let state = Arc::new(RwLock::new(State::new()));

	let (sender, receiver) = unbounded();

	let is_exit = Arc::new(AtomicCell::new(false));

	{
		clone_all!(log, state, is_exit, sender);
		#[allow(deprecated)]
		actix_web::server::new(move || {
			let app_state =
				AppState::new(log.clone(), state.clone(), is_exit.clone(), sender.clone());
			actix_web::App::with_state(app_state)
				.resource("/create-queue", |r| {
					r.method(Method::PUT).with_async(create_queue)
				})
				.resource("/delete-queue", |r| {
					r.method(Method::DELETE).with_async(delete_queue)
				})
				.resource("/enqueue", |r| r.method(Method::POST).with_async(enqueue))
				.resource("/show", |r| r.method(Method::POST).with_async(show))
				.resource("/stop", |r| r.method(Method::DELETE).f(exit))
				.default_resource(|r| {
					r.f(|r| {
						error!(r.state().log, "unknown route"; "method"=>r.method().as_str(), "path"=>r.uri().path());
						format!("unknown route {} {}", r.method().as_str(), r.uri().path())
					})
				})
		})
		.start_incoming(listener.incoming(), false);
	}

	{
		clone_all!(log, receiver, sender);
		thread::scope(move |scope| {
			{
				clone_all!(log);
				scope.spawn(move |_| {
					info!(log, "scheduler thread spawned");
					while !is_exit.load() {
						// check for available schedule
						info!(log, "waiting for msg ..");

						let msg = receiver.recv();
						let mut state = state.write().unwrap();
						match msg {
							Ok(Message::TrySchedule(q_name)) => {
								info!(log, "msg: TrySchedule"; "q"=>&q_name);
								// check validity of msg
								if let Some(queue) = state.queues.get_mut(&q_name) {
									queue.try_schedule(sender.clone());
								} else {
									continue;
								}
							}
							Ok(Message::JobRunning(msg)) => {
								info!(log, "msg: JobRunning"; "q"=>&msg.q_name, "job"=>&msg.job_id);
								// check validity of msg
								// TODO: use queue_id
								if let Some(queue) = state.queues.get_mut(&msg.q_name) {
									if let Some(mut job) = queue.running_jobs.get_mut(&msg.job_id) {
										job.status = JobStatus::Running { pid: msg.pid };
									} else {
										error!(
											log,
											"race condition; got pid information, but job is not running"; "job_id"=>msg.job_id
										);
									}
								} else {
									warn!(log, "job running, but its queue has gone");
								}
							}
							Ok(Message::JobFinished(msg)) => {
								info!(log, "msg: JobFinished"; "q"=>&msg.q_name, "job"=>&msg.job_id);
								// check validity of msg
								// TODO: use queue_id
								if let Some(queue) = state.queues.get_mut(&msg.q_name) {
									if let Some(mut job) = queue.running_jobs.remove(&msg.job_id) {
										job.status = JobStatus::Finished {
											exit_status: msg.status,
											duration: 0,
										};
										for (res_type, req) in &job.require {
											*queue
												.available
												.get_mut(res_type)
												.unwrap()
												.0
												.get_mut(
													&job.allocation.as_ref().unwrap()[res_type],
												)
												.unwrap() += *req;
										}
										queue.past_jobs.push(job);
										queue.try_schedule(sender.clone());
									} else {
										error!(
											log,
											"race condition; finished job is not in running_jobs"; "job_id"=>msg.job_id
										);
									}
								} else {
									warn!(log, "job finished, but its queue has gone");
								}
							}
							Err(e) => {
								error!(log, "channel disconnected"; "err"=>%e);
								break;
							}
						};
					}
				});
			}

			info!(log, "Listening on the socket.");
			let _ = sys.run();
		})
		.unwrap(); // join crossbeam threads
	}
}

#[cfg(test)]
mod tests {
	use super::QueueSpecification;

	#[test]
	fn test_queue() {
		let q = &serde_json::from_str::<QueueSpecification>(
			r#"
			{
				"name": "qs",
				"resources": {
					"gpu": 4,
					"hdd": [64,32,128],
					"cpu": {
						"xeon1": 16,
						"xeon2": 8
					}
				}
			}
		"#,
		)
		.unwrap()
		.build();

		assert_eq!(q.name, "qs");
		assert_eq!(
			q.resources.get("gpu").unwrap().0,
			[
				("0".to_string(), 1.into()),
				("1".to_string(), 1.into()),
				("2".to_string(), 1.into()),
				("3".to_string(), 1.into())
			]
			.iter()
			.cloned()
			.collect()
		);

		assert_eq!(
			q.resources.get("hdd").unwrap().0,
			[
				("0".to_string(), 64.into()),
				("1".to_string(), 32.into()),
				("2".to_string(), 128.into()),
			]
			.iter()
			.cloned()
			.collect()
		);

		assert_eq!(
			q.resources.get("cpu").unwrap().0,
			[
				("xeon1".to_string(), 16.into()),
				("xeon2".to_string(), 8.into()),
			]
			.iter()
			.cloned()
			.collect()
		);
	}
}
