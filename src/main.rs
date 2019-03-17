#![feature(try_blocks)]
#![feature(label_break_value)]
#[macro_use]
extern crate clap;

use prettytable::{cell, color, row, Attr, Cell, Row, Table};
use serde_json::{json, Value};
#[allow(unused_imports)]
use slog::{error, info, o, trace, warn, Drain};
use std::io::Read;
use std::process::Stdio;

mod cli;
mod objects;
mod server;

fn send_json(url: &str, method: &str, obj: &Value) -> String {
	let payload = obj.to_string();
	let mut payload = payload.as_bytes();
	let mut easy = curl::easy::Easy::new();
	easy.unix_socket("/tmp/grass.sock")
		.expect("cannot connect to server");
	easy.url(url).unwrap();
	match method {
		"put" => easy.put(true).unwrap(),
		"get" => easy.get(true).unwrap(),
		"delete" => easy.custom_request("DELETE").unwrap(),
		"post" => easy.post(true).unwrap(),
		_ => unimplemented!(),
	}

	easy.post_field_size(payload.len() as u64).unwrap();

	let mut buf = Vec::new();
	{
		let mut transfer = easy.transfer();
		transfer
			.read_function(|buf| Ok(payload.read(buf).unwrap()))
			.unwrap();
		transfer
			.write_function(|data| {
				buf.extend_from_slice(data);
				Ok(data.len())
			})
			.unwrap();
		transfer.perform().unwrap();
	}
	let res = std::str::from_utf8(&buf[..]).unwrap().to_string();
	return res;
}

fn get_logger(file: Option<std::fs::File>) -> slog::Logger {
	let decorator = slog_term::TermDecorator::new().build();
	let drain_term = slog_term::CompactFormat::new(decorator).build().fuse();

	if let Some(file) = file {
		let decorator = slog_term::PlainDecorator::new(file);
		let drain_file = slog_term::CompactFormat::new(decorator).build().fuse();

		let drain = slog::Duplicate(drain_term, drain_file).fuse();
		let drain = slog_async::Async::new(drain).build().fuse();

		slog::Logger::root(drain, o!())
	} else {
		let drain = slog_async::Async::new(drain_term).build().fuse();
		slog::Logger::root(drain, o!())
	}
}

fn main() {
	let args = cli::build_cli().get_matches();

	let (sub, matches) = args.subcommand();
	let matches = matches.unwrap();
	match sub.as_ref() {
		"start" => {
			let log = get_logger(None);
			info!(log, "Starting daemon .."; "pid" => "/tmp/grass.pid");
			let self_path = std::env::args().next().unwrap();
			std::process::Command::new(&self_path)
				.arg("daemon")
				.stdin(Stdio::null())
				.stdout(Stdio::null())
				.stderr(Stdio::null())
				.spawn()
				.expect("Daemon process failed to start.");
		}
		"stop" => {
			send_json("http://localhost/stop", "delete", &json!({}));
		}
		"create-queue" => 'c: {
			let log = get_logger(None);
			let name = matches.value_of("name").unwrap().clone();

			if let Some(_) = matches.value_of("file") {
				unimplemented!();
			}
			let j = matches.value_of("json").unwrap();
			let resources = match json5::from_str::<Value>(j) {
				Ok(val) => val,
				Err(e) => {
					error!(log, "fail to parse json"; "err"=>%e);
					break 'c;
				}
			};
			let payload = json!({"name": name, "resources": resources});
			send_json("http://localhost/create-queue", "put", &payload);
		}
		"delete-queue" => {
			let log = get_logger(None);
			let value = json!({
				"name": matches.value_of("name").unwrap().clone(),
				"confirmed": matches.is_present("confirmed"),
			});
			let res = send_json("http://localhost/delete-queue", "delete", &value);
			info!(log, "response"; "msg"=>%res);
		}
		"enqueue" => 'e: {
			let log = get_logger(None);
			// grass enqueue --queue q --cwd . --gpu 1 --cpu 0.5 -- python train.py ..
			let cmd: Vec<&str> = matches.values_of("cmd").unwrap().collect();
			let envs: Vec<&str> = matches
				.values_of("env")
				.map(|x| x.collect())
				.unwrap_or(Vec::new());
			let req = if let Some(j) = matches.value_of("json") {
				match json5::from_str::<Value>(j) {
					Ok(val) => val,
					Err(e) => {
						error!(log, "fail to parse json"; "err"=>%e);
						break 'e;
					}
				}
			} else {
				json!({})
			};

			let cwd = if let Some(path) = matches.value_of("cwd") {
				std::fs::canonicalize(path)
			} else {
				std::fs::canonicalize(
					std::env::current_dir().expect("cannot read current working direrctory"),
				)
			}
			.unwrap()
			.to_str()
			.expect("cwd is invalid utf-8")
			.to_string();

			let value = json!({
				"queue": matches.value_of("name").unwrap().clone(),
				"cwd": cwd,
				"cmd": cmd,
				"envs": envs,
				"require": req,
			});
			info!(log, "enqueue"; "payload" => %value);
			let res = send_json("http://localhost/enqueue", "post", &value);
			info!(log, "response"; "msg"=>res);
		}
		"show" => {
			let log = get_logger(None);
			let value = if let Some(q) = matches.value_of("queue") {
				json!({ "queue": q })
			} else {
				json!({})
			};
			info!(log, "show"; "payload" => %value);
			let res = send_json("http://localhost/show", "post", &value);

			if matches.is_present("json") {
				info!(log, "response"; "msg"=>serde_json::from_str::<Value>(&res).and_then(|v| serde_json::to_string_pretty(&v)).as_ref().unwrap_or(&res));
			} else {
				// for each queue
				if let Ok(val) = serde_json::from_str::<Value>(&res) {
					if let Some(obj) = val.as_object() {
						for (q_name, q_obj) in obj {
							println!();
							let mut t = term::stdout().unwrap();
							t.fg(term::color::WHITE).unwrap();
							t.attr(term::Attr::Bold).unwrap();

							writeln!(t, "[Queue: {}]", q_name).unwrap();

							t.reset().unwrap();
							let mut table = Table::new();
							table.add_row(row![
								"job_id",
								"status",
								"command",
								"allocation",
								"result"
							]);

							for (job_status, q_iter) in &mut [
								(
									Cell::new("Pending"),
									&mut (&q_obj["future_jobs"]).as_array().unwrap().iter()
										as (&mut dyn std::iter::Iterator<Item = &Value>),
								),
								(
									Cell::new("Running")
										.with_style(Attr::ForegroundColor(color::YELLOW)),
									&mut (&q_obj["running_jobs"]).as_object().unwrap().values()
										as (&mut dyn std::iter::Iterator<Item = &Value>),
								),
								(
									Cell::new("Finished")
										.with_style(Attr::ForegroundColor(color::GREEN)),
									&mut (&q_obj["past_jobs"]).as_array().unwrap().iter()
										as (&mut dyn std::iter::Iterator<Item = &Value>),
								),
							] {
								for job_obj in q_iter {
									let job = job_obj.as_object().unwrap();
									let cmd: String = job["cmd"]
										.as_array()
										.unwrap()
										.iter()
										.map(|x| x.as_str().unwrap().to_string())
										.collect::<Vec<String>>()
										.join(" ");
									table.add_row(Row::new(vec![
										Cell::new(job["id"].as_str().unwrap()),
										job_status.clone(),
										Cell::new(&cmd),
										Cell::new(
											serde_json::to_string(&job["allocation"])
												.as_ref()
												.map(String::as_str)
												.unwrap_or(""),
										),
										Cell::new(
											job.get("result")
												.and_then(|x| serde_json::to_string(x).ok())
												.as_ref()
												.map(String::as_str)
												.unwrap_or("?"),
										),
									]));
								}
							}

							if table.len() == 0 {
								println!("no jobs");
							}
							table.printstd();
						}
					}

					// job_id | status | command | allocation
				}
			}
		}
		"daemon" => {
			let log = get_logger(Some(
				std::fs::File::create("/tmp/grass.log").expect("cannot open /tmp/grass.log"),
			));
			info!(log, "This is a daemon process.");
			let mut lock = pidlock::Pidlock::new("/tmp/grass.pid");
			if let Err(e) = lock.acquire() {
				error!(log, "Failed to get lockfile. Quit"; "err"=>format!("{:?}", e), "lock"=>"/tmp/grass.pid");
				drop(log);
				std::process::exit(1);
			}

			let _ = std::fs::remove_file("/tmp/grass.sock");
			server::run(log.clone()); // block

			lock.release().expect("fail to release lock");
			info!(log, "Quit.");
		}
		"dashboard" => unimplemented!(),
		_ => unreachable!(),
	};
}
