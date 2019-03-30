use rust_decimal::Decimal;
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::path::PathBuf;

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
#[derive(Debug, Serialize, Clone, Default)]
pub struct ResourceTypeCapacity(pub HashMap<String, Decimal>);

/// ResourceRequirement = {
/// 	gpu: 3
/// 	ram: 256
/// }
#[derive(Clone,Debug, Serialize, Deserialize, Default)]
pub struct ResourceRequirement(pub HashMap<String, Decimal>);

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct WorkerCapacity(pub HashMap<String, ResourceTypeCapacity>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Allocation(pub HashMap<String, String>);

impl WorkerCapacity {
	pub fn can_run_job(&self, req: &ResourceRequirement) -> Option<Allocation> {
		let mut allocation = Allocation(HashMap::<String, String>::new());
		let chk = req.0.iter().all(|(res_type, amount)| {
			if self.0.contains_key(res_type) {
				// hdd1, hdd2, ..
				for (res_instance, res_capacity) in &self.0[res_type].0 {
					if res_capacity >= amount {
						allocation.0.insert(res_type.to_string(), res_instance.to_string()); // HDD: HDD1
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
			*self.0.get_mut(res_type).unwrap().0.get_mut(&allocation.0[res_type]).unwrap() -= *req;
		}
	}

	pub fn restore(&mut self, job: &Job) {
		let allocation = job.allocation.as_ref().unwrap();
		for (res_type, req) in &job.spec.require.0 {
			*self.0.get_mut(res_type).unwrap().0.get_mut(&allocation.0[res_type]).unwrap() += *req;
		}
	}
}

impl<'de> Deserialize<'de> for ResourceTypeCapacity {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let value: Value = Deserialize::deserialize(deserializer)?;
		match value {
			Value::Number(n) => {
				if let Some(n) = n.as_u64() {
					let mut m = HashMap::new();
					for i in 0..n {
						m.insert(i.to_string(), 1.into());
					}
					Ok(ResourceTypeCapacity(m))
				} else {
					Err(de::Error::custom("It should be a single integer"))
				}
			}
			Value::String(s) => {
				if let Ok(n) = &s.parse::<u32>() {
					let mut m = HashMap::new();
					for i in 0..*n {
						m.insert(i.to_string(), 1.into());
					}
					Ok(ResourceTypeCapacity(m))
				} else {
					Err(de::Error::custom("It should be a single integer"))
				}
			}
			Value::Array(arr) => {
				let mut m = HashMap::new();
				// check if all items are numbers
				for (i, item) in arr.iter().enumerate() {
					if item.is_number() {
						m.insert(i.to_string(), Decimal::from_str(&item.to_string()).unwrap());
					} else {
						return Err(de::Error::custom("List has a non-number"));
					}
				}
				Ok(ResourceTypeCapacity(m))
			}
			Value::Object(obj) => {
				let mut m = HashMap::new();
				for (k, value) in obj.iter() {
					let v: Decimal = if let Some(num) = value.as_f64() {
						num::FromPrimitive::from_f64(num).unwrap()
					} else if let Some(s) = value.as_str() {
						Decimal::from_str(s).map_err(|_| {
							de::Error::custom(format!(
								"Map value is not a number: {}",
								&value.to_string()
							))
						})?
					} else {
						Err(de::Error::custom(format!(
							"Map value is not a number: {}",
							&value.to_string()
						)))?
					};
					m.insert(k.to_string(), v);
				}
				Ok(ResourceTypeCapacity(m))
			}
			_ => Err(de::Error::custom(
				"Unsupported resource capacity. It should be int, list, or dictionary",
			)),
		}
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

#[derive(Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
	host: String,
	runnning_since: chrono::DateTime<chrono::Utc>,

}
impl WorkerInfo {
	fn uptime(&self) -> std::time::Duration {
		std::time::Duration::new(0,5)
	}
}


/// Job has information about running context.
/// Job consumes resources from a queue, and returns back when finished.
///
#[derive(Clone, Serialize, Deserialize)]
pub struct Job {
	pub id: String,
	pub spec: JobSpecification,
	pub status: JobStatus,
	pub allocation: Option<Allocation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobSpecification {
	pub cwd: PathBuf,
	pub cmd: Vec<String>,
	pub envs: Vec<(String, String)>,
	pub require: ResourceRequirement,
}

impl JobSpecification {
	pub fn build(self) -> Job {
		let id = uuid::Uuid::new_v4().to_hyphenated().to_string();
		Job {
			id,
			spec:self,
			status: JobStatus::Pending,
			allocation: Default::default(),
		}
	}
}
