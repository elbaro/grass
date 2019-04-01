#![feature(await_macro, async_await, futures_api)]
#![feature(try_blocks)]
#![feature(label_break_value)]
#![feature(associated_type_defaults, proc_macro_hygiene)]
#![allow(unused_imports, dead_code)]

#[macro_use]
extern crate clap;

use prettytable::{cell, color, row, Attr, Cell, Row, Table};
#[allow(unused_imports)]
use slog::{error, info, o, trace, warn};
use std::path::PathBuf;
use std::process::Stdio;
use tarpc::context;

use futures::{FutureExt,TryFutureExt};
use futures::compat::Future01CompatExt;
use futures::compat::Stream01CompatExt;

mod cli;
mod logger;
mod objects;

mod broker;
mod daemon;
mod rpc;
mod worker;

use objects::{Job, JobSpecification, JobStatus, ResourceRequirement, WorkerCapacity};

use app_dirs::{get_app_root, AppInfo};
const APP_INFO: AppInfo = AppInfo {
	name: "grass",
	author: "elbaro",
};
#[derive(serde::Serialize, serde::Deserialize)]
struct AppConfig {
	master: Option<std::net::SocketAddr>,
}

impl AppConfig {
	fn default() -> AppConfig {
		AppConfig { master: None }
	}
	fn load() -> Result<AppConfig, Box<dyn std::error::Error>> {
		let path = get_app_root(app_dirs::AppDataType::UserConfig, &APP_INFO)?.join("grass.toml");
		let string = match std::fs::read_to_string(&path) {
			Ok(s) => s,
			Err(e) => {
				eprintln!("cannot read config file at {:#?}, err: {}", &path, e);
				return Ok(AppConfig::default());
			}
		};
		toml::from_str::<AppConfig>(&string).map_err(|e| e.into())
	}
	fn save(&self) -> Result<(), Box<dyn std::error::Error>> {
		let path = get_app_root(app_dirs::AppDataType::UserConfig, &APP_INFO)?.join("grass.toml");
		let s = toml::to_string_pretty(self)?;
		std::fs::write(path, s)?;
		Ok(())
	}
}

fn main() {
	let args = cli::build_cli().get_matches();
	let mut _app_config = AppConfig::load();

	let (sub, matches) = args.subcommand();
	let matches = matches.unwrap();

	let log = if sub == "daemon" {
		logger::init_logger(Some("/tmp/grass.log"))
	} else {
		logger::init_logger(None)
	};

	match sub.as_ref() {
		"start" => {
			// default === broker(bind:localhost), worker(connect:lcoalhost)
			//             --no-broker  --bind
			//                                       --no-worker --connect
			//

			info!(log, "Starting daemon in background."; "pid" => "/tmp/grass.pid");

			let self_path = std::env::args().next().unwrap();
			let mut cmd = std::process::Command::new(&self_path);
			cmd.arg("daemon")
				.stdin(Stdio::null())
				.stdout(Stdio::null())
				.stderr(Stdio::null());

			if let Some(master) = matches.value_of("master") {
				cmd.arg("--master").arg(master);
			}

			cmd.spawn().expect("Daemon process failed to start.");
		}
		"stop" => {
			info!(log, "Stopping daemon in background.");
			tokio::run_async(
				async {
					let mut client = await!(daemon::new_daemon_client()).unwrap();
					await!(client.stop(context::current())).unwrap();
				},
			);
		}
		"enqueue" => 'e: {
			// grass enqueue --queue q --cwd . --gpu 1 --cpu 0.5 -- python train.py ..
			let cmd: Vec<String> = matches
				.values_of("cmd")
				.unwrap()
				.map(str::to_string)
				.collect();
			let envs: Vec<(String, String)> = matches
				.values_of("env")
				.map(|x| {
					x.map(|expr| {
						// input: A=B
						// output: (A,B)
						let mut s = expr.splitn(2, '=');
						let a = s.next().expect("invalid env");
						let b = s.next().expect("invalid env");
						(a.to_string(), b.to_string())
					})
					.collect()
				})
				.unwrap_or_default();
			let req: ResourceRequirement =
				json5::from_str(matches.value_of("json").unwrap_or_default()).unwrap();

			let cwd: PathBuf = if let Some(path) = matches.value_of("cwd") {
				std::fs::canonicalize(path)
			} else {
				std::fs::canonicalize(
					std::env::current_dir().expect("cannot read current working direrctory"),
				)
			}
			.unwrap();

			let job_spec = JobSpecification {
				cmd,
				cwd,
				envs,
				require: req,
			};

			info!(log, "enqueue"; "payload" => ?job_spec);

			tokio::run_async(
				async {
					let mut client =
						await!(broker::new_broker_client("localhost:7500".parse().unwrap()))
							.unwrap();
					await!(client.job_enqueue(context::current(), job_spec)).unwrap();
				},
			);
		}
		"show" => {
			// output example:
			//
			// [local daemon]
			// not running
			// pid: 11
			// broker: none
			// broker: listening 127.0.0.1:11
			// worker: none
			// worker: 2 brokers, 127.0.0.1:11, remote.com:111

			// if matches.is_present("json") {
			// 	// string -> json::Value -> pretty string
			// 	info!(log, "response"; "msg"=>serde_json::from_str::<Value>(&res).and_then(|v| serde_json::to_string_pretty(&v)).as_ref().unwrap_or(&res));
			// } else {
			// 	if let Ok(queues) = serde_json::from_str::<HashMap<String, Queue>>(&res) {
			// 		// for each queue
			// 		for (q_name, q) in &queues {
			// 			println!();
			// 			let mut t = term::stdout().unwrap();
			// 			t.fg(term::color::WHITE).unwrap();
			// 			t.attr(term::Attr::Bold).unwrap();

			// 			writeln!(t, "[Queue: {}]", q_name).unwrap();

			// 			t.reset().unwrap();
			// 			let mut table = Table::new();
			// 			table.add_row(row!["job_id", "status", "command", "allocation", "result"]);

			// 			for q_iter in &mut [
			// 				&mut q.future_jobs.iter()
			// 					as (&mut dyn std::iter::Iterator<Item = &Job>),
			// 				&mut q.running_jobs.values()
			// 					as (&mut dyn std::iter::Iterator<Item = &Job>),
			// 				&mut q.past_jobs.iter() as (&mut dyn std::iter::Iterator<Item = &Job>),
			// 			] {
			// 				for job in q_iter {
			// 					let cmd: String = job.cmd.join(" ");

			// 					let (status_cell, result) = match &job.status {
			// 						JobStatus::Created => (Cell::new("Pending"), "".to_string()),
			// 						JobStatus::Running { pid } => (
			// 							Cell::new("Running")
			// 								.with_style(Attr::ForegroundColor(color::YELLOW)),
			// 							format!("pid: {}", pid),
			// 						),
			// 						JobStatus::Finished {
			// 							exit_status: Ok(()),
			// 							..
			// 						} => (
			// 							Cell::new("Success")
			// 								.with_style(Attr::ForegroundColor(color::GREEN)),
			// 							"-".to_string(),
			// 						),
			// 						JobStatus::Finished {
			// 							exit_status: Err(err),
			// 							..
			// 						} => (
			// 							Cell::new("Failed")
			// 								.with_style(Attr::ForegroundColor(color::RED)),
			// 							err.to_string(),
			// 						),
			// 					};

			// 					let allocation = job
			// 						.allocation
			// 						.as_ref()
			// 						.map(|x| serde_json::to_string(&x).unwrap())
			// 						.unwrap_or("".to_string());

			// 					// wrap cmd and result
			// 					let cmd = textwrap::fill(&cmd, 30);
			// 					let result = textwrap::fill(&result, 20);
			// 					let allocation = textwrap::fill(&allocation, 20);

			// 					table.add_row(Row::new(vec![
			// 						Cell::new(&job.id[..8]),
			// 						status_cell,
			// 						Cell::new(&cmd),
			// 						Cell::new(&allocation),
			// 						Cell::new(&result),
			// 					]));
			// 				}
			// 			}

			// 			if table.len() == 0 {
			// 				println!("no jobs");
			// 			}
			// 			table.printstd();
			// 		}

			// 		// job_id | status | command | allocation
			// 	}
			// }
		}
		"daemon" => {
			info!(log, "This is a daemon process.");
			let mut lock = pidlock::Pidlock::new("/tmp/grass.pid");
			if let Err(e) = lock.acquire() {
				error!(log, "Failed to get lockfile. Quit"; "err"=>format!("{:?}", e), "lock"=>"/tmp/grass.pid");
				drop(log);
				std::process::exit(1);
			}

			let _ = std::fs::remove_file("/tmp/grass.sock");

			let resources: WorkerCapacity = matches
				.value_of("json")
				.map(|j| json5::from_str(j).unwrap())
				.unwrap_or_default();

			let broker_config = Some(broker::BrokerConfig {
				bind_addr: "127.0.0.1:7500".parse().unwrap(),
			});
			let worker_config = Some(worker::WorkerConfig {
				broker_addr: "127.0.0.1:7500".parse().unwrap(),
				resources,
			});

			let daemon = daemon::Daemon::new(broker_config, worker_config);
			daemon.run_sync();

			// server::run(log.clone()); // block
			// daemon::

			lock.release().expect("fail to release lock");
			info!(log, "Quit.");
		}
		"dashboard" => unimplemented!(),
		_ => unreachable!(),
	};
}
