use actix_web::{http::Method, HttpMessage, HttpRequest, HttpResponse, Responder};
use decimal::d128;
use futures::future::Future;
use futures::stream::Stream;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use tokio_uds::UnixListener;

struct Resource {
	name: String,
	capacities: Vec<(String, d128)>,
}

impl Resource {
	fn to_json(&self) -> json::JsonValue {
		let mut cap = json::JsonValue::new_object();
		for (k, v) in &self.capacities {
			cap[k] = v.to_string().into();
		}
		json::object! {
			"name" => self.name.clone(),
			"capacities" => cap,
		}
	}

	/// There are 3 types of resource specification in json.
	/// int 4 === [1,1,1,1]
	/// list [64,64,32] === {"0":64,"1":64,"2":32}
	/// dict {"HDD0":128,"HDD2":64,"HDD5":32}
	fn from_json(name: &str, value: &json::JsonValue) -> Result<Resource, Box<std::error::Error>> {
		let mut v = Vec::<(String, d128)>::new();

		use json::JsonValue::{Array, Number, Object};
		use std::iter::Iterator;

		match value {
			Number(n) => {
				let f: f64 = (*n).into();
				if f.fract() != 0.0 {
					Err("A single number should be integer")?;
				}
				let n = f as u32;
				for i in 0..n {
					v.push((i.to_string(), d128!(1)));
				}
			}
			Array(arr) => {
				// check if all items are numbers
				if !arr.iter().all(|x| x.is_number()) {
					Err("List has a non-number")?;
				}

				for (i, item) in arr.iter().enumerate() {
					v.push((i.to_string(), d128::from_str(&item.dump()).unwrap()));
				}
			}
			Object(_obj) => {
				unimplemented!();
				// obj.iter()
				// for (i,item) in  {
				// 	let f:f64 = item.into();
				// 	v.push((i.to_string(), f));
				// }
			}
			_ => {
				Err("Unsupported resource capacity. It should be int, list, or dictionary")?;
			}
		}

		Ok(Resource {
			name: name.to_string(),
			capacities: v,
		})
	}
}

enum JobStatus {
	Created,
	Pending,
	Completed,
	Failed,
}

struct Job {
	id: String,
	args: Vec<String>,
	require: HashMap<String, d128>,
	status: JobStatus,
}

impl Job {
	/// {
	/// 	"name": "queue",
	/// 	"resources": {
	/// 		"gpu": 8,
	/// 	}
	/// }
	///
	fn from_json(value: json::JsonValue) -> Result<Job, Box<std::error::Error>> {
		let id = uuid::Uuid::new_v4().to_hyphenated().to_string();

		let args = if let json::JsonValue::Array(vec) = &value["args"] {
			vec.iter()
				.map(|x| x.as_str().map(|s| s.to_string()))
				.collect::<Option<Vec<String>>>()
				.ok_or("non-string in args")?
		} else {
			Err("args should be a list")?
		};

		let require: HashMap<String, d128> = if let json::JsonValue::Object(obj) = &value["require"]
		{
			obj.iter()
				.map(|(k, v)| (k.to_string(), d128::from_str(&v.dump()).unwrap()))
				.collect()
		} else {
			Err("requirement field is not obj")?
		};

		Ok(Job {
			id,
			args,
			require,
			status: JobStatus::Created,
		})
	}
	fn spawn(&self, allocation: &HashMap<String, String>) -> Result<(), Box<std::error::Error>> {
		std::process::Command::new(&self.args[0])
			.args(
				&self.args[1..]
					.iter()
					.map(|x| strfmt::strfmt(x, allocation))
					.collect::<Result<Vec<String>, _>>()?,
			)
			.spawn()
			.expect("Job failed to start");

		Ok(())
	}
}

struct Queue {
	name: String,
	resources: HashMap<String, Resource>,
}
impl Queue {
	fn new(name: String, resources: HashMap<String, Resource>) -> Queue {
		Queue { name, resources }
	}
	fn to_json(&self) -> json::JsonValue {
		let mut res = json::JsonValue::new_object();
		for (k, v) in &self.resources {
			res[k] = v.to_json();
		}
		json::object! {
			"name" => self.name.clone(),
			"resources" => res,
		}
	}
	/// {
	/// 	"name": "queue",
	/// 	"resources": {
	/// 		"gpu": 8,
	/// 	}
	/// }
	///
	fn from_json(value: json::JsonValue) -> Result<Queue, Box<std::error::Error>> {
		let name = value["name"].as_str().ok_or("no name")?.to_string();
		if let json::JsonValue::Object(obj) = &value["resources"] {
			let resources: HashMap<String, Resource> = obj
				.iter()
				.map(|(k, v)| (k.to_string(), Resource::from_json(k, v).unwrap()))
				.collect();
			return Ok(Queue { name, resources });
		} else {
			Err("no resources")?
		}
	}
}

struct State {
	queues: HashMap<String, Queue>,
}
impl State {
	fn new() -> State {
		State {
			queues: HashMap::new(),
		}
	}
	fn to_json(&self) -> json::JsonValue {
		let mut arr = json::JsonValue::new_array();
		for (_, q) in &self.queues {
			arr.push(q.to_json()).unwrap();
		}
		arr
	}
}

pub struct AppState {
	state: Arc<RwLock<State>>,
}
impl AppState {
	pub fn new() -> AppState {
		AppState {
			state: Arc::new(RwLock::new(State::new())),
		}
	}
}

fn create_queue(
	r: HttpRequest<AppState>,
) -> impl Future<Item = HttpResponse, Error = actix_web::Error> {
	let state = r.state().state.clone();
	r.payload()
		.concat2()
		.from_err::<actix_web::Error>()
		.and_then(move |body| {
			let value = json::parse(std::str::from_utf8(&body).unwrap()).unwrap();
			match Queue::from_json(value) {
				Ok(q) => {
					let mut state = state.write().unwrap();
					if state.queues.contains_key(&q.name) {
						Ok(HttpResponse::from("q already exists"))
					} else {
						state.queues.insert(q.name.clone(), q);
						Ok(HttpResponse::from("created"))
					}
				}
				Err(_e) => Ok(HttpResponse::from("error parsing json")),
			}
		})
}
fn delete_queue(
	r: HttpRequest<AppState>,
) -> impl Future<Item = HttpResponse, Error = actix_web::Error> {
	let state = r.state().state.clone();
	r.payload()
		.concat2()
		.from_err::<actix_web::Error>()
		.and_then(move |body| {
			let value = json::parse(std::str::from_utf8(&body).unwrap()).unwrap();
			if let Some(name) = value["name"].as_str() {
				let mut state = state.write().unwrap();
				if state.queues.contains_key(name) {
					state.queues.remove(name);
					Ok(HttpResponse::from("deleted"))
				} else {
					Ok(HttpResponse::from("cannot find q"))
				}
			} else {
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
			let value = json::parse(std::str::from_utf8(&body).unwrap()).unwrap();
			if let Some(name) = value["name"].as_str() {
				let mut state = state.write().unwrap();
				if state.queues.contains_key(name) {
					state.queues.remove(name);
					Ok(HttpResponse::from("deleted"))
				} else {
					Ok(HttpResponse::from("cannot find q"))
				}
			} else {
				Ok(HttpResponse::from("missing name field"))
			}
		})
}
fn show(r: HttpRequest<AppState>) -> impl Future<Item = HttpResponse, Error = actix_web::Error> {
	let state = r.state().state.clone();
	r.payload()
		.concat2()
		.from_err::<actix_web::Error>()
		.and_then(move |_body| {
			// let value = json::parse(std::str::from_utf8(&body).unwrap()).unwrap();
			let state = state.write().unwrap();
			Ok(HttpResponse::Ok()
				.content_type("application/json")
				.body(state.to_json().dump()))
		})
}
fn exit(_r: &HttpRequest<AppState>) -> impl Responder {
	actix::System::current().stop();
	"bye"
}

pub fn run() {
	let listener = UnixListener::bind("/tmp/grass.sock").expect("bind failed");
	let sys = actix::System::new("unix-socket");
	#[allow(deprecated)]
	actix_web::server::new(move || {
		actix_web::App::with_state(AppState::new())
			.resource("/create-queue/", |r| {
				r.method(Method::PUT).with_async(create_queue)
			})
			.resource("/delete-queue", |r| {
				r.method(Method::DELETE).with_async(delete_queue)
			})
			.resource("/enqueue", |r| r.method(Method::POST).with_async(enqueue))
			.resource("/show", |r| r.method(Method::GET).with_async(show))
			.resource("/stop", |r| r.method(Method::DELETE).f(exit))
	})
	.start_incoming(listener.incoming(), false);

	let _ = sys.run();
}
