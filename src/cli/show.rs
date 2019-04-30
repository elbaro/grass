use colored::*;
use tarpc::context;


use crate::broker::BrokerInfo;
use crate::daemon::DaemonInfo;

use crate::objects::JobStatus;
use crate::{broker, compat, daemon};
fn print_std(daemon_info: DaemonInfo, broker_info: BrokerInfo) {
	use prettytable::{cell, color, row, Attr, Cell, Row, Table};
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
			table.add_row(row!["hostname", "os", "uptime", "queues"]);
			for worker in &info.workers {
				table.add_row(row![
					worker.node_spec.hostname,
					format!("{} ({})", &worker.node_spec.os, worker.node_spec.os_release), // os
					worker // uptime
						.node_spec
						.get_uptime()
						.to_std()
						.map(|d| timeago::Formatter::new().convert(d))
						.unwrap_or("time sync mismatch".to_string()),
					worker
						.queue_infos
						.iter() // queues
						.map(|q_info| format!("{} ({}? running)", &q_info.name, &q_info.running))
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
					Cell::new("Running").with_style(Attr::ForegroundColor(color::YELLOW)),
					format!("pid: {}", pid),
				),
				JobStatus::Finished {
					exit_status: Ok(()),
					..
				} => (
					Cell::new("Success").with_style(Attr::ForegroundColor(color::GREEN)),
					"-".to_string(),
				),
				JobStatus::Finished {
					exit_status: Err(err),
					..
				} => (
					Cell::new("Failed").with_style(Attr::ForegroundColor(color::RED)),
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

fn print_interactive(
	daemon_info: DaemonInfo,
	broker_info: BrokerInfo,
) -> Result<(), failure::Error> {
	use super::tui_events::{Event, Events};
	use std::io;
	use termion::event::Key;

	use termion::input::MouseTerminal;
	use termion::raw::IntoRawMode;
	use termion::screen::AlternateScreen;
	use tui::backend::TermionBackend;
	use tui::layout::{Constraint, Direction, Layout};
	use tui::style::{Color, Modifier, Style};
	use tui::widgets::{Block, Borders, Paragraph, Row, Table, Text, Widget};
	use tui::Terminal;

	/// current page
	enum View {
		Root { selection: usize },
		Job(usize),
		Worker(usize),
	}

	let stdout = io::stdout().into_raw_mode()?;
	let stdout = MouseTerminal::from(stdout);
	let stdout = AlternateScreen::from(stdout);
	let backend = TermionBackend::new(stdout);
	let mut terminal = Terminal::new(backend)?;
	terminal.hide_cursor()?;

	let mut view = View::Root { selection: 0 };
	let mut index = 0;
	let count = 1;

	let events = Events::new();

	'interactive: loop {
		terminal.draw(|mut f| {
			let size = f.size();
			let mut i = 0;
			match &view {
				View::Root { selection } => {
					Block::default()
						.title("Broker")
						.borders(Borders::ALL)
						.render(&mut f, size);

					let layout = Layout::default()
						.constraints(
							[Constraint::Percentage(20), Constraint::Percentage(80)].as_ref(),
						)
						.margin(3)
						.split(f.size());

					let header = vec!["hostname", "os", "uptime", "queues"];
					let rows: Vec<_> = broker_info
						.workers
						.iter()
						.map(|worker| {
							let row = Row::StyledData(
								worker.display_columns().into_iter(),
								if i == *selection {
									Style::default().fg(Color::Yellow).modifier(Modifier::BOLD)
								} else {
									Style::default()
								},
							);
							i += 1;
							row
						})
						.collect();

					Table::new(header.into_iter(), rows.into_iter())
						.block(Block::default().borders(Borders::ALL).title("workers"))
						.widths(&[30, 30, 30, 30])
						.render(&mut f, layout[0]);

					let header = vec![
						"created",
						"queue",
						"job id",
						"status",
						"command",
						"allocation",
						"result",
					];
					let rows: Vec<_> = broker_info
						.jobs
						.iter()
						.map(|job| {

							let row = Row::StyledData(
								job.display_columns().into_iter(),
								if i == *selection {
									Style::default().fg(Color::Yellow).modifier(Modifier::BOLD)
								} else {
									Style::default()
								},
							);
							i += 1;
							row
						})
						.collect();

					Table::new(header.into_iter(), rows.into_iter())
						.block(Block::default().borders(Borders::ALL).title("jobs"))
						.widths(&[10, 10, 10, 10, 80, 10, 10])
						.render(&mut f, layout[1]);
				}
				View::Job(_id) => {
					Paragraph::new(vec![Text::raw("job with id")].iter()).render(&mut f, size);
					// stdout
					// stderr
				}
				View::Worker(_id) => {
					Paragraph::new(vec![Text::raw("worker with id")].iter()).render(&mut f, size);
					// stdout
					// stderr
				}
			}
		})?;

		match &view {
			View::Root { selection } => {
				match events.next()? {
					Event::Input(key) => match key {
						Key::Esc | Key::Char('q') => {
							break 'interactive;
						}
						Key::Char('\n') => {
							// if job
							if *selection < broker_info.workers.len() {
								view = View::Worker(*selection);
							} else {
								view = View::Job(*selection - broker_info.workers.len());
							}

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
						_ => {}
					},
					// _ => {unreachable!();}
				};
			}
			View::Worker(id) => match events.next()? {
				Event::Input(key) => match key {
					Key::Esc | Key::Char('q') => {
						view = View::Root { selection: 0 };
					}
					_ => {}
				},
			},
			View::Job(id) => match events.next()? {
				Event::Input(key) => match key {
					Key::Esc | Key::Char('q') => {
						view = View::Root { selection: 0 };
					}
					_ => {}
				},
			},
			_ => {}
		}
	}
	Ok(())
}

pub fn run(broker_addr: std::net::SocketAddr, interactive: bool) -> Result<(), failure::Error> {
	let (daemon_info, broker_info) = compat::tokio_try_run(async move {
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
	})?;

	if interactive {
		print_interactive(daemon_info, broker_info)?;
	} else {
		print_std(daemon_info, broker_info);
	}

	Ok(())
}
