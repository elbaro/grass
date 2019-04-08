use clap::{App, AppSettings, Arg, ArgGroup, SubCommand};
// use indoc::indoc;

clap::arg_enum! {
	#[derive(Debug)]
	enum Database {
		Sqlite,
		Postgres,
	}
}

pub fn build_cli() -> App<'static, 'static> {
	App::new("grass")
		.version(clap::crate_version!())
		.author("github.com/elbaro/grass")
		.about("GRES job scheduler")
		.setting(AppSettings::SubcommandRequiredElseHelp)
		.subcommand(
			// about daemon
			SubCommand::with_name("start")
				.about("start a daemon in background")
				.arg(
					Arg::with_name("db")
						.long("db")
						.possible_values(&Database::variants()),
				),
		)
		.subcommand(
			// about daemon
			SubCommand::with_name("stop")
				.about("stop a daemon in background")
				.arg(Arg::with_name("queue").long("queue").takes_value(true))
				.arg(Arg::with_name("status").long("status").takes_value(true))
				.arg(Arg::with_name("confirmed").long("confirmed")),
		)
		.subcommand(
			SubCommand::with_name("create-queue")
				.about("Create a queue on the local worker (via daemon)")
				.arg(Arg::with_name("name").index(1).required(true))
				.arg(Arg::with_name("cwd").long("cwd").takes_value(true))
				.arg(Arg::with_name("cmd").multiple(true).required(true))
				.arg(
					Arg::with_name("env")
						.short("e")
						.long("env")
						.multiple(true)
						.takes_value(true),
				)
				.arg(
					Arg::with_name("capacity")
						.long("capacity")
						.takes_value(true),
				)
				.setting(AppSettings::TrailingVarArg),
		)
		.subcommand(
			SubCommand::with_name("delete-queue")
				.about("Delete a queue from the local worker")
				.arg(Arg::with_name("name").index(1).required(true)),
		)
		.subcommand(
			// about broker
			// method1. TrailingVarArg: last positional (with or w/o --)
			// method2. AllowExternalSubcommand: cannot use --
			SubCommand::with_name("enqueue")
				.about("Enqueue new job")
				.arg(Arg::with_name("q_name").index(1).required(true))
				.arg(Arg::with_name("cwd").long("cwd").takes_value(true))
				.arg(Arg::with_name("sync").long("sync"))
				.arg(Arg::with_name("require").long("require").takes_value(true))
				.arg(Arg::with_name("cmd").multiple(true).required(true))
				.arg(
					Arg::with_name("env")
						.short("e")
						.long("env")
						.multiple(true)
						.takes_value(true),
				)
				.arg(Arg::with_name("broker").long("broker").takes_value(true))
				.setting(AppSettings::TrailingVarArg),
		)
		.subcommand(
			// about daemon, broker, or worker
			// 1) show
			// 3) show --queue q1
			SubCommand::with_name("show")
				.about("Show the job status")
				// .arg(Arg::with_name("queue").long("queue").takes_value(true))
				.arg(Arg::with_name("json").long("json"))
				.arg(Arg::with_name("table").long("table"))
				.arg(Arg::with_name("broker").long("broker").takes_value(true))
				.group(ArgGroup::with_name("print-style").args(&["json", "table"])),
		)
		// .subcommand(
		// 	SubCommand::with_name("dashboard")
		// 		.arg(Arg::with_name("bind").long("bind").takes_value(true)),
		// )
		.subcommand(
			// broker+worker: --bind, --connect
			// broker: --bind, --no-worker
			// worker: --no-broker, --connect
			SubCommand::with_name("daemon")
				.arg(Arg::with_name("bind").long("bind").takes_value(true))
				.arg(Arg::with_name("no-broker").long("no-broker"))
				.arg(Arg::with_name("connect").long("connect").takes_value(true))
				.arg(Arg::with_name("no-worker").long("no-worker"))
				.group(ArgGroup::with_name("broker").args(&["bind", "no-broker"]))
				.group(ArgGroup::with_name("worker").args(&["connect", "no-worker"])),
		)
}
