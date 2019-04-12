#![recursion_limit = "128"]
#![feature(await_macro, async_await, futures_api)]
#![feature(try_blocks, arbitrary_self_types, type_ascription)]
#![feature(label_break_value)]
#![feature(associated_type_defaults, proc_macro_hygiene)]
#![allow(unused_imports, dead_code)]

#[macro_use]
extern crate clap;

use colored::*;
use prettytable::{cell, color, row, Attr, Cell, Row, Table};
#[allow(unused_imports)]
use slog::{error, info, o, trace, warn};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use tarpc::context;

use futures::compat::Executor01CompatExt;
use futures::compat::Future01CompatExt;
use futures::compat::Stream01CompatExt;
use futures::{FutureExt, TryFutureExt};

mod broker;
mod cli;
mod compat;
mod daemon;
mod logger;
mod objects;
mod oneshot;
mod rpc;
mod worker;
use objects::{Job, JobSpecification, JobStatus, QueueCapacity, ResourceRequirement};
use worker::QueueConfig;

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
		toml::from_str::<AppConfig>(&string).map_err(std::convert::Into::into)
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
		// logger::init_logger(Some("/tmp/grass.log"))
		logger::create_logger(Some("/tmp/grass.log"))
	} else {
		logger::create_logger(None)
	};

	let _log_guard = slog_scope::set_global_logger(log);
	let log = slog_scope::logger();

	match sub {
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

			// validate
			if let Some(resources) = matches.value_of("resources") {
				let _: QueueCapacity =
					json5::from_str(resources).expect("fail to parse arg resources");
				cmd.arg("--resources").arg(resources);
			}

			cmd.spawn().expect("Daemon process failed to start.");
		}
		"stop" => {
			info!(log, "[Command] Stopping a local daemon.");
			tarpc::init(tokio::executor::DefaultExecutor::current().compat());
			compat::tokio_run(
				async move {
					let mut client = await!(daemon::new_daemon_client()).unwrap();
					info!(log, "Stopping.");
					await!(client.stop(context::current())).unwrap();
					info!(log, "Done.");
				},
			);
		}
		"enqueue" => 'e: {
			// grass enqueue --cwd . --req "{gpu:1}" -- python train.py ..
			let q_name: String = matches.value_of("q_name").unwrap().to_string();
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
			let req: ResourceRequirement = matches
				.value_of("require")
				.map(|j| json5::from_str(j).expect("wrong json5 format"))
				.unwrap_or_default();

			let broker_addr: SocketAddr = matches
				.value_of("broker")
				.unwrap_or("127.0.0.1:7500")
				.parse()
				.expect("broker address should be ip:port");

			let job_spec = JobSpecification {
				q_name,
				cmd,
				envs,
				require: req,
			};

			compat::tokio_run(
				async move {
					let mut client = await!(broker::new_broker_client(broker_addr)).unwrap();
					info!(log, "Enqueueing."; "job_spec" => ?job_spec);
					await!(client.job_enqueue(context::current(), job_spec)).unwrap();
					info!(log, "Enqueued");
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

			// [broker]
			// pending jobs

			// [worker: hostname]
			//     os:
			//     os_dist:
			//     [queue: 1]
			//         available: ,
			//         [running jobs]
			//     [queue: 2]
			//         [running jobs]

			let broker_addr: SocketAddr = matches
				.value_of("broker")
				.unwrap_or("127.0.0.1:7500")
				.parse()
				.expect("broker address should be ip:port");

			compat::tokio_run(
				async move {
					{
						println!("{}", "[Daemon]".bold());

						let mut client = await!(daemon::new_daemon_client()).unwrap();
						let info = await!(client.info(context::current())).unwrap();
						if info.broker {
							println!("Broker: Running");
						} else {
							println!("Broker: -");
						}
						if let Some(worker) = info.worker.as_ref() {
							println!("Worker: {}", worker.broker_addr);
						} else {
							println!("Worker: -");
						}
					}

					// broker
					{
						let mut client = await!(broker::new_broker_client(broker_addr)).unwrap();
						let info = await!(client.info(context::current())).unwrap();
						println!("{}", format!("[Broker @ {:?}]", &info.bind_addr).bold());

						{
							use chrono::TimeZone;
							// workers
							let mut table = Table::new();
							table.add_row(row![
								"hostname",
								"uptime",
								"heartbeat",
								"load",
								"queues"
							]);
							for worker in &info.workers {
								table.add_row(row![
									worker.node_spec.hostname,
									worker
										.node_spec
										.get_uptime()
										.to_std()
										.map(|d| timeago::Formatter::new().convert(d))
										.unwrap_or("time sync mismatch".to_string())
								]);
							}
							table.printstd();
							println!();
						}

						{
							// queues
							// let mut table = Table::new();
							// table.add_row(row![
							// 	"q_name",
							// 	"cwd",
							// 	"cmd",
							// 	"env",
							// 	"available",
							// 	"running"
							// ]);
						}

						let mut table = Table::new();
						table.add_row(row![
							"created",
							"queue",
							"job id",
							"status",
							"command",
							"allocation",
							"result"
						]);

						for job in &info.jobs {
							let cmd: String = job.spec.cmd.join(" ");

							let (status_cell, result) = match &job.status {
								JobStatus::Pending => (Cell::new("Pending"), "".to_string()),
								JobStatus::Running { pid } => (
									Cell::new("Running")
										.with_style(Attr::ForegroundColor(color::YELLOW)),
									format!("pid: {}", pid),
								),
								JobStatus::Finished {
									exit_status: Ok(()),
									..
								} => (
									Cell::new("Success")
										.with_style(Attr::ForegroundColor(color::GREEN)),
									"-".to_string(),
								),
								JobStatus::Finished {
									exit_status: Err(err),
									..
								} => (
									Cell::new("Failed")
										.with_style(Attr::ForegroundColor(color::RED)),
									err.to_string(),
								),
							};

							let allocation: String = job
								.allocation
								.as_ref()
								.map(|x| serde_json::to_string(&x).unwrap())
								.unwrap_or_default();

							// wrap cmd and result
							let cmd = textwrap::fill(&cmd, 30);
							let result = textwrap::fill(&result, 20);
							let allocation = textwrap::fill(&allocation, 20);

							table.add_row(Row::new(vec![
								Cell::new(
									&job.created_at
										.with_timezone(&chrono::Local)
										.format("%Y-%m-%d %P %l:%M:%S")
										.to_string(),
								),
								Cell::new(&job.spec.q_name),
								Cell::new(&job.id[..8]),
								status_cell,
								Cell::new(&cmd),
								Cell::new(&allocation),
								Cell::new(&result),
							]));
						}

						if table.is_empty() {
							println!("no jobs");
						}
						table.printstd();
					}
				}, // async move
			); // tokio_run
		}
		"daemon" => {
			// let log = slog_scope::logger();
			let mut lock = pidlock::Pidlock::new("/tmp/grass.pid");
			if let Err(e) = lock.acquire() {
				error!(log, "Failed to get lockfile. Quit"; "err"=>format!("{:?}", e), "lock"=>"/tmp/grass.pid");
				// drop(_log_guard);
				panic!("asdf");
			}

			let _ = std::fs::remove_file("/tmp/grass.sock");

			let broker_config = if matches.is_present("no-broker") {
				None
			} else {
				Some(broker::BrokerConfig {
					bind_addr: matches
						.value_of("bind")
						.unwrap_or("127.0.0.1:7500")
						.parse()
						.expect("fail to parse --bind address"),
					cert: matches.value_of("cert").map(|s| s.into()),
					cert_pass: matches.value_of("cert-pass").map(|s| s.into()),
				})
			};

			let worker_config = if matches.is_present("no-worker") {
				None
			} else {
				Some(worker::WorkerConfig {
					broker_addr: matches
						.value_of("connect")
						.unwrap_or("127.0.0.1:7500")
						.parse()
						.expect("fail to parse --connect address"),
				})
			};

			let daemon = daemon::Daemon::new(broker_config, worker_config);
			daemon.run_sync();

			// server::run(log.clone()); // block
			// daemon::

			lock.release().expect("fail to release lock");
			info!(log, "Quit.");
		}
		"create-queue" => {
			let cwd: PathBuf = if let Some(path) = matches.value_of("cwd") {
				std::fs::canonicalize(path)
			} else {
				std::fs::canonicalize(
					std::env::current_dir().expect("cannot read current working direrctory"),
				)
			}
			.unwrap();

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

			let capacity: QueueCapacity = matches
				.value_of("capacity")
				.map(|j| QueueCapacity::from_json_str(&j).expect("invalid json5"))
				.unwrap_or_default();

			let config = QueueConfig {
				name: matches.value_of("name").unwrap().to_string(),
				cwd,
				cmd,
				envs,
				capacity,
			};

			tarpc::init(tokio::executor::DefaultExecutor::current().compat());
			compat::tokio_run(
				async move {
					let mut client = await!(daemon::new_daemon_client()).unwrap();
					await!(client.create_queue(context::current(), config)).unwrap();
					info!(log, "Done");
				},
			);
		}
		"delete-queue" => {
			let name = matches.value_of("name").unwrap().to_string();
			tarpc::init(tokio::executor::DefaultExecutor::current().compat());
			compat::tokio_run(
				async move {
					let mut client = await!(daemon::new_daemon_client()).unwrap();
					await!(client.delete_queue(context::current(), name)).unwrap();
					info!(log, "Done");
				},
			);
		}
		"dashboard" => unimplemented!(),
		_ => unreachable!(),
	};
}
