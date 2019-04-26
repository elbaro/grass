use std::io;
use termion::event::Key;
use termion::raw::IntoRawMode;
use termion::input::MouseTerminal;
use termion::screen::AlternateScreen;
use tui::Terminal;
use tui::backend::TermionBackend;
use tui::widgets::{Widget, Block, Borders};
use tui::layout::{Layout, Constraint, Direction};

use colored::*;
use prettytable::{cell, color, row, Attr, Cell, Row, Table};
use tarpc::context;

use crate::{broker, compat, daemon};
use crate::daemon::DaemonInfo;
use crate::broker::BrokerInfo;
use crate::objects::{Job, JobSpecification, JobStatus, QueueCapacity, ResourceRequirement};

use super::tui_events::{Event, Events};

fn print_std(daemon_info: DaemonInfo, broker_info: BrokerInfo) {
	{
		let info = daemon_info;
		println!("{}", "[Daemon]".bold());
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
	{
		let info = broker_info;
		println!("{}", format!("[Broker @ {:?}]", &info.bind_addr).bold());

		{
			use chrono::TimeZone;
			// workers
			let mut table = Table::new();
			table.add_row(row![
				"hostname", "os", "uptime",
				// "heartbeat",
				// "load",
				"queues"
			]);
			for worker in &info.workers {
				table.add_row(row![
					worker.node_spec.hostname,
					format!(
						"{} ({})",
						&worker.node_spec.os, worker.node_spec.os_release
					), // os
					worker // uptime
						.node_spec
						.get_uptime()
						.to_std()
						.map(|d| timeago::Formatter::new().convert(d))
						.unwrap_or("time sync mismatch".to_string()),
					worker
						.queue_infos
						.iter() // queues
						.map(|q_info| format!(
							"{} ({}? running)",
							&q_info.name, &q_info.running
						))
						.collect::<Vec<_>>()
						.join(", ")
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
}

fn print_interactive(daemon_info: DaemonInfo, broker_info: BrokerInfo) -> Result<(), failure::Error> {
	let stdout = io::stdout().into_raw_mode()?;
	let stdout = MouseTerminal::from(stdout);
	let stdout = AlternateScreen::from(stdout);
    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
	terminal.hide_cursor()?;

	let mut index = 0;
	let count = 1;

	loop {
		terminal.draw(|mut f| {
			let size = f.size();
			Block::default()
			    .title("Block")
			    .borders(Borders::ALL)
			    .render(&mut f, size);

			Block::default()
				.title("Block")
				.borders(Borders::ALL)
				.render(&mut f, size);
			Block::default()
				.title("Block 2")
				.borders(Borders::ALL)
				.render(&mut f, size);


		})?;

		let events = Events::new();

		match events.next()? {
            Event::Input(key) => match key {
                Key::Char('q') => {
                    break;
                }
                Key::Down => {
                    index += 1;
                    if index > count - 1 {
                        index = 0;
                    }
                }
                Key::Up => {
                    if index > 0 {
                        index -= 1;
                    } else {
                        index = count - 1;
                    }
                }
                _ => {
				}
            },
            _ => {}
        };
	}
	Ok(())
}

pub fn run(broker_addr: std::net::SocketAddr, interactive: bool) -> Result<(), failure::Error> {
	let (daemon_info, broker_info) = compat::tokio_try_run(
		async move {
			let daemon_info: DaemonInfo = {
				let mut client = await!(daemon::new_daemon_client())?;
				await!(client.info(context::current()))?
			};

			// broker
			let broker_info: BrokerInfo = {
				let mut client = await!(broker::new_broker_client(broker_addr))?;
				await!(client.info(context::current()))?
			};

			Ok((daemon_info, broker_info))
		}
	)?;

	if interactive {
		print_interactive(daemon_info, broker_info)?;
	} else {
		print_std(daemon_info, broker_info);
	}

	Ok(())
}
