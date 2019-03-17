use rust_decimal::Decimal;
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

/// represents a capacity for one resource type, e.g. gpu.
/// {
/// 	"gpu": 3, // one
/// 	"cpu": [10,20,30], // one
/// 	"hdd": {0:10, 1:20, 2:30}, // one
/// }
#[derive(Debug, Serialize, Clone)]
pub struct ResourceTypeCapacity(pub HashMap<String, Decimal>);

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
					if !value.is_number() {
						return Err(de::Error::custom("map value is not a number"));
					}
					m.insert(
						k.to_string(),
						Decimal::from_str(&value.to_string()).unwrap(),
					);
				}
				Ok(ResourceTypeCapacity(m))
			}
			_ => Err(de::Error::custom(
				"Unsupported resource capacity. It should be int, list, or dictionary",
			)),
		}
	}
}

#[derive(Clone, Serialize, Deserialize)]
pub enum JobStatus {
	Created,
	// Pending,
	Running {
		pid: i32,
	},
	// Completed,
	// Failed,
	Finished {
		exit_status: Result<(), String>,
		duration: u32,
	},
}
