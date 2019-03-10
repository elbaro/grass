use actix_web::{http::Method, HttpMessage, HttpRequest, HttpResponse, Responder};
use clone_all::clone_all;
use crossbeam::{
	atomic::AtomicCell,
	channel::{Receiver, Sender},
	sync::{Parker, Unparker},
	thread,
};
use decimal::d128;
use futures::future::Future;
use futures::stream::Stream;
use serde::Serialize;
use serde_json::Value;
use slog::{error, info};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use tokio_uds::UnixListener;

enum Message {
	TrySchedule(String),
	JobFinished(String),
}

#[derive(Debug, Serialize, Clone)]
pub struct Resource {
	name: String,
	capacities: HashMap<String, d128>,
}

impl Resource {
	// fn to_json(&self) -> Value {
	// 	let mut cap = serde_json::map::Map::new();
	// 	for (k, v) in &self.capacities {
	// 		cap[k] = v.to_string().into();
	// 	}
	// 	json!({
	// 		"name" : self.name.clone(),
	// 		"capacities" : cap,
	// 	})
	// }

	/// name: 'gpu'
	/// value: num, [], {}

	/// There are 3 types of resource specification in json.
	/// int 4 === [1,1,1,1]
	/// list [64,64,32] === {"0":64,"1":64,"2":32}
	/// dict {"HDD0":128,"HDD2":64,"HDD5":32}
	fn from_json(name: &str, value: &Value) -> Result<Resource, Box<std::error::Error>> {
		let mut v = Vec::<(String, d128)>::new();

		use std::iter::Iterator;

		match value {
			Value::Number(n) => {
				if let Some(n) = n.as_u64() {
					for i in 0..n {
						v.push((i.to_string(), d128!(1)));
					}
				} else {
					Err("It should be a single integer")?;
				}
			}
			Value::Array(arr) => {
				// check if all items are numbers
				if !arr.iter().all(|x| x.is_number()) {
					Err("List has a non-number")?;
				}

				for (i, item) in arr.iter().enumerate() {
					v.push((i.to_string(), d128::from_str(&item.to_string()).unwrap()));
				}
			}
			Value::Object(obj) => {
				for (k, value) in obj.iter() {
					if !value.is_number() {
						Err("dict value is not a number")?
					}
					v.push((k.to_string(), d128::from_str(&value.to_string()).unwrap()));
				}
			}
			_ => {
				Err("Unsupported resource capacity. It should be int, list, or dictionary")?;
			}
		}

		Ok(Resource {
			name: name.to_string(),
			capacities: v.iter().cloned().collect(),
		})
	}
}

#[derive(Clone, Serialize)]
pub enum JobStatus {
	Created,
	// Pending,
	Running,
	// Completed,
	// Failed,
}

#[derive(Clone, Serialize)]
pub struct Job {
	id: String,
	cwd: PathBuf,
	cmd: Vec<String>,
	envs: Vec<(String, String)>,
	require: HashMap<String, d128>,
	status: JobStatus,
	allocation: Option<HashMap<String, String>>,
}

impl Job {
	/// {
	/// 	"name": "queue",
	/// 	"resources": {
	/// 		"gpu": 8,
	/// 	}
	/// }
	fn from_json(value: &Value) -> Result<Job, Box<std::error::Error>> {
		let id = uuid::Uuid::new_v4().to_hyphenated().to_string();

		let cwd = if let Some(c) = value.get("cwd") {
			PathBuf::from(c.as_str().expect("cwd is not string"))
		} else {
			std::fs::canonicalize(
				std::env::current_dir().expect("cannot read current working direrctory"),
			)
			.unwrap()
		};

		let cmd = if let Value::Array(vec) = &value["cmd"] {
			vec.iter()
				.map(|x| x.as_str().map(|s| s.to_string()))
				.collect::<Option<Vec<String>>>()
				.ok_or("non-string in cmd")?
		} else {
			Err("cmd should be a list")?
		};

		let require: HashMap<String, d128> = if let Value::Object(obj) = &value["require"] {
			obj.iter()
				.map(|(k, v)| (k.to_string(), d128::from_str(&v.to_string()).unwrap()))
				.collect()
		} else {
			Err("require field is not obj")?
		};

		Ok(Job {
			id,
			cwd,
			cmd,
			envs: Vec::new(),
			require,
			status: JobStatus::Created,
			allocation: None,
		})
	}
	fn spawn(
		&mut self,
		unparker: Unparker,
		// s: Sender<Message>,
	) -> Result<(), Box<std::error::Error>> {
		self.status = JobStatus::Running;
		let job: Job = self.clone();
		std::thread::spawn(move || -> Result<(), Box<std::error::Error + Send>> {
			let allocation = job.allocation.as_ref().unwrap();
			let _status = std::process::Command::new(&job.cmd[0])
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
				.status();
			unparker.unpark();
			// s.send();
			Ok(())
		});

		Ok(())
	}
}

#[derive(Serialize)]
pub struct Queue {
	name: String,
	id: String,
	resources: HashMap<String, Resource>,
	available: HashMap<String, Resource>,
	future_jobs: VecDeque<Job>,
	running_jobs: HashMap<String, Job>,
	past_jobs: Vec<Job>,
}
impl Queue {
	fn from_json(value: &Value) -> Result<Queue, Box<std::error::Error>> {
		let id = uuid::Uuid::new_v4().to_hyphenated().to_string();
		let name = value["name"].as_str().ok_or("no name")?.to_string();
		if let Value::Object(obj) = &value["resources"] {
			let resources: HashMap<String, Resource> = obj
				.iter()
				.map(|(k, v)| (k.to_string(), Resource::from_json(k, v).unwrap()))
				.collect();
			let available = resources.clone();
			return Ok(Queue {
				id,
				name,
				resources,
				available,
				future_jobs: VecDeque::new(),
				running_jobs: HashMap::new(),
				past_jobs: Vec::new(),
			});
		} else {
			Err("no resources")?
		}
	}

	fn check_availability(&self, job: &Job) -> Option<HashMap<String, String>> {
		let mut allocation = HashMap::<String, String>::new();
		let chk = job.require.iter().all(|(res_type, amount)| {
			if self.available.contains_key(res_type) {
				// hdd1, hdd2, ..
				for (res_instance, res_capacity) in &self.available[res_type].capacities {
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

	fn run(&mut self, mut job: Job, unparker: Unparker) {
		// decrease availability
		for (res_type, req) in &job.require {
			*self
				.available
				.get_mut(res_type)
				.unwrap()
				.capacities
				.get_mut(&job.allocation.as_ref().unwrap()[res_type])
				.unwrap() -= *req;
		}
		job.spawn(unparker).unwrap();
		self.running_jobs.insert(job.id.to_string(), job);
	}
}

#[derive(Serialize)]
pub struct State {
	queues: HashMap<String, Queue>,
}
impl State {
	fn new() -> State {
		State {
			queues: HashMap::new(),
		}
	}
	// fn to_json(&self) -> Value {
	// 	let mut arr = Vec::<Value>::new();
	// 	for (_, q) in &self.queues {
	// 		arr.push(q.to_json());
	// 	}
	// 	Value::Array(arr)
	// }
}

#[derive(Clone)]
pub struct AppState {
	log: slog::Logger,
	state: Arc<RwLock<State>>,
	unparker: Unparker,
	is_exit: Arc<AtomicCell<bool>>,
}
impl AppState {
	pub fn new(
		log: slog::Logger,
		state: Arc<RwLock<State>>,
		unparker: Unparker,
		is_exit: Arc<AtomicCell<bool>>,
	) -> AppState {
		info!(log, "--- AppState is created ---");
		AppState {
			log: log,
			state: state,
			unparker: unparker,
			is_exit: is_exit,
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
				Queue::from_json(&serde_json::from_str::<Value>(std::str::from_utf8(&body)?)?)?
			};
			match q {
				Ok(q) => {
					let mut state = state.write().unwrap();
					if state.queues.contains_key(&q.name) {
						Ok(HttpResponse::from("q already exists"))
					} else {
						state.queues.insert(q.name.clone(), q);
						r.state().unparker.unpark();
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
					r.state().unparker.unpark();
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
	r.payload()
		.concat2()
		.from_err::<actix_web::Error>()
		.and_then(move |body| {
			let value: Value = serde_json::from_str(std::str::from_utf8(&body).unwrap()).unwrap();
			if let Some(queue) = value["queue"].as_str() {
				let mut state = state.write().unwrap();
				if state.queues.contains_key(queue) {
					let q = state.queues.get_mut(queue).unwrap();

					let job = Job::from_json(&value).unwrap();
					q.future_jobs.push_back(job);
					r.state().unparker.unpark();
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
	r.state().unparker.unpark();
	"bye"
}

pub fn run(log: slog::Logger) {
	let listener = UnixListener::bind("/tmp/grass.sock").expect("bind failed");
	let sys = actix::System::new("unix-socket");
	let parker = Parker::new();
	let unparker = parker.unparker();
	let state = Arc::new(RwLock::new(State::new()));

	let is_exit = Arc::new(AtomicCell::new(false));

	{
		clone_all!(log, unparker, state, is_exit);
		#[allow(deprecated)]
		actix_web::server::new(move || {
			let app_state = AppState::new(
				log.clone(),
				state.clone(),
				unparker.clone(),
				is_exit.clone(),
			);
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
		clone_all!(log, unparker);
		thread::scope(move |scope| {
			{
				clone_all!(log);
				scope.spawn(move |_| {
					info!(log, "scheduler thread spawned");
					while !is_exit.load() {
						// check for available schedule
						info!(log, "parking");
						parker.park();
						info!(log, "unparked");

						let mut state = state.write().unwrap();
						for (_name, queue) in state.queues.iter_mut() {
							if !queue.future_jobs.is_empty() {
								if let Some(allocation) =
									queue.check_availability(&queue.future_jobs[0])
								{
									let mut job = queue.future_jobs.pop_front().unwrap();
									job.allocation = Some(allocation);
									queue.run(job, unparker.clone());
								} else {
									error!(log, "job still pending");
								}
							}
						}
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
	use super::Queue;
	use decimal::d128;

	#[test]
	fn test_queue() {
		let q = Queue::from_json(
			&serde_json::from_str::<serde_json::value::Value>(
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
			.unwrap(),
		)
		.unwrap();

		assert_eq!(q.name, "qs");
		assert_eq!(
			q.resources.get("gpu").unwrap().capacities,
			[
				("0".to_string(), d128::from(1)),
				("1".to_string(), d128::from(1)),
				("2".to_string(), d128::from(1)),
				("3".to_string(), d128::from(1))
			]
			.iter()
			.cloned()
			.collect()
		);

		assert_eq!(
			q.resources.get("hdd").unwrap().capacities,
			[
				("0".to_string(), d128::from(64)),
				("1".to_string(), d128::from(32)),
				("2".to_string(), d128::from(128)),
			]
			.iter()
			.cloned()
			.collect()
		);

		assert_eq!(
			q.resources.get("cpu").unwrap().capacities,
			[
				("xeon1".to_string(), d128::from(16)),
				("xeon2".to_string(), d128::from(8)),
			]
			.iter()
			.cloned()
			.collect()
		);
	}
}
