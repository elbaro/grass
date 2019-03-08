#[macro_use]
extern crate clap;

#[allow(unused_imports)]
use slog::{error, info, o, trace, warn, Drain};
use std::io::Read;

// use json::jsonValue;

mod cli;
mod server;

fn main() {
	let decorator = slog_term::TermDecorator::new().build();
	let drain = slog_term::CompactFormat::new(decorator).build().fuse();
	let drain = slog_async::Async::new(drain).build().fuse();
	let log = slog::Logger::root(drain, o!());

	let args = cli::build_cli().get_matches();

	let (sub, matches) = args.subcommand();
	let matches = matches.unwrap();
	match sub.as_ref() {
		"start" => {
			info!(log, "Starting daemon .."; "pid" => "/tmp/grass.pid");
			let self_path = std::env::args().next().unwrap();
			std::process::Command::new(&self_path)
				.arg("daemon")
				.spawn()
				.expect("Daemon process failed to start.");
		}
		"stop" => {}
		"create-queue" => {
			let res = json::JsonValue::new_object();
			let value = json::object! {
				"name" => matches.value_of("queue").unwrap().clone(),
				"resources" => res,
			};
			let data = value.dump();

			let mut easy = curl::easy::Easy::new();
			easy.unix_socket("/tmp/grass.sock")
				.expect("cannot connect to server");
			easy.url("http://localhost/create-queue	").unwrap();
			easy.put(true).unwrap();
			let mut transfer = easy.transfer();
			transfer
				.read_function(|buf| Ok(data[..].as_bytes().read(buf).unwrap()))
				.unwrap();
			transfer.perform().unwrap();

			// let client = reqwest::Client::new();
			// let res = client
			// 	.put("http://httpbin.org/post")
			// 	.json(&map)
			// 	.send()
			// 	.expect("fail to send request");
		}
		"delete-queue" => {}
		"enqueue" => {}
		"show" => {}
		"daemon" => {
			info!(log, "This is a daemon process.");
			let mut lock = pidlock::Pidlock::new("/tmp/grass.pid");
			if let Err(e) = lock.acquire() {
				error!(log, "Failed to get lockfile. Quit"; "err"=>format!("{:?}", e), "lock"=>"/tmp/grass.pid");
				drop(log);
				std::process::exit(1);
			}

			let _ = std::fs::remove_file("/tmp/grass.sock");
			server::run(); // block

			lock.release().expect("fail to release lock");
			info!(log, "Quit.");
		}
		_ => unreachable!(),
	};
}
