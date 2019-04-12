use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

use futures::channel::mpsc::UnboundedSender;

/// represents a capacity for one resource type, e.g. gpu.
/// {
/// 	"gpu": 3, // one
/// 	"cpu": [10,20,30], // one
/// 	"hdd": {0:10, 1:20, 2:30}, // one
/// }
///
/// ResourceTypeCapacity = {
/// 	0: 256
/// 	1: 256
/// 	2: 128
/// }
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ResourceTypeCapacity(pub HashMap<String, f64>);

/// ResourceRequirement = {
/// 	gpu: 3
/// 	ram: 256
/// }
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ResourceRequirement(pub HashMap<String, f64>);

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct QueueCapacity(pub HashMap<String, ResourceTypeCapacity>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Allocation(pub HashMap<String, String>);

impl QueueCapacity {
	pub fn can_run_job(&self, req: &ResourceRequirement) -> Option<Allocation> {
		let mut allocation = Allocation(HashMap::<String, String>::new());
		let chk = req.0.iter().all(|(res_type, amount)| {
			if self.0.contains_key(res_type) {
				// hdd1, hdd2, ..
				for (res_instance, res_capacity) in &self.0[res_type].0 {
					if res_capacity >= amount {
						allocation
							.0
							.insert(res_type.to_string(), res_instance.to_string()); // HDD: HDD1
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

	pub fn consume(&mut self, job: &Job) {
		let allocation = job.allocation.as_ref().unwrap();
		for (res_type, req) in &job.spec.require.0 {
			let v = self
				.0
				.get_mut(res_type)
				.unwrap()
				.0
				.get_mut(&allocation.0[res_type])
				.unwrap();
			*v = ((*v - *req) * 1000.0).round() / 1000.0;
		}
	}

	pub fn restore(&mut self, job: &Job) {
		let allocation = job.allocation.as_ref().unwrap();
		for (res_type, req) in &job.spec.require.0 {
			let v = self
				.0
				.get_mut(res_type)
				.unwrap()
				.0
				.get_mut(&allocation.0[res_type])
				.unwrap();
			*v = ((*v + *req) * 1000.0).round() / 1000.0;
		}
	}
}

use std::sync::RwLock;
struct JobRunningGuard {
	job: Job,
	worker_capacity: RwLock<QueueCapacity>,
}

impl JobRunningGuard {
	fn new(job: Job, worker_capacity: RwLock<QueueCapacity>) -> JobRunningGuard {
		{
			let mut cap = worker_capacity.write().unwrap();
			cap.consume(&job);
		}
		JobRunningGuard {
			job,
			worker_capacity,
		}
	}
}

impl Drop for JobRunningGuard {
	fn drop(&mut self) {
		// 1. restore resources
		// 2. send a msg to update job status
		{
			let mut cap = self.worker_capacity.write().unwrap();
			cap.restore(&self.job);
		}
	}
}

impl QueueCapacity {
	pub fn from_json_str(s: &str) -> Result<QueueCapacity, &'static str> {
		let value: serde_json::Value = json5::from_str(s).map_err(|_| "invalid json5")?;
		let value = value.as_object().expect("root json is not an object");

		let mut cap = HashMap::new();

		for (res_type, value) in value.iter() {
			let parsed = match value {
				Value::Number(n) => {
					if let Some(n) = n.as_u64() {
						let mut m = HashMap::new();
						for i in 0..n {
							m.insert(i.to_string(), 1.0);
						}
						ResourceTypeCapacity(m)
					} else {
						Err("A number is not u64")?
					}
				}
				Value::String(s) => {
					if let Ok(n) = &s.parse::<u32>() {
						let mut m = HashMap::new();
						for i in 0..*n {
							m.insert(i.to_string(), 1.0);
						}
						ResourceTypeCapacity(m)
					} else {
						Err("A string is not u32")?
					}
				}
				Value::Array(arr) => {
					let mut m = HashMap::new();
					// check if all items are numbers
					for (i, item) in arr.iter().enumerate() {
						if let Some(f) = item.as_f64() {
							m.insert(i.to_string(), f);
						} else {
							return Err("List has a non-number");
						}
					}
					ResourceTypeCapacity(m)
				}
				Value::Object(obj) => {
					let mut m = HashMap::new();
					for (k, value) in obj.iter() {
						let v: f64 = if let Some(num) = value.as_f64() {
							num
						} else if let Some(s) = value.as_str() {
							s.parse::<f64>()
								.map_err(|_| "Map value is not a number: {}")?
						} else {
							Err("Map value is not a number: {}")?
						};
						m.insert(k.to_string(), v);
					}
					ResourceTypeCapacity(m)
				}
				_ => Err("Unsupported resource capacity. It should be int, list, or dictionary")?,
			};

			cap.insert(res_type.to_string(), parsed);
		}
		Ok(QueueCapacity(cap))
	}
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JobStatus {
	Pending,
	Running {
		pid: u32,
	},
	Finished {
		exit_status: Result<(), String>,
		duration: u32,
	},
}

/// Job has information about running context.
/// Job consumes resources from a queue, and returns back when finished.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
	pub id: String,
	pub created_at: chrono::DateTime<chrono::Utc>,
	pub spec: JobSpecification,
	pub status: JobStatus,
	pub allocation: Option<Allocation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobSpecification {
	pub q_name: String,
	pub cmd: Vec<String>,
	pub envs: Vec<(String, String)>,
	pub require: ResourceRequirement,
}

impl JobSpecification {
	pub fn build(self) -> Job {
		let id = uuid::Uuid::new_v4().to_hyphenated().to_string();
		Job {
			id,
			created_at: chrono::Utc::now(),
			spec: self,
			status: JobStatus::Pending,
			allocation: Default::default(),
		}
	}
}
