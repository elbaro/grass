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
		.version("prealpha")
		.author("github.com/elbaro/grass")
		.about("GRES job scheduler")
		.setting(AppSettings::SubcommandRequiredElseHelp)
		.subcommand(
			SubCommand::with_name("start").arg(
				Arg::with_name("db")
					.long("db")
					.possible_values(&Database::variants()),
			),
		)
		.subcommand(
			SubCommand::with_name("stop")
				.arg(Arg::with_name("queue").long("queue").takes_value(true))
				.arg(Arg::with_name("status").long("status").takes_value(true))
				.arg(Arg::with_name("confirmed").long("confirmed")),
		)
		.subcommand(
			// candidates:
			// grass create-queue default --gpu 4 --cpu 2
			// grass create-queue default --resource gpu=4 --resource cpu=2
			// grass create-queue default --json '{gpu:4,cpu:2}'
			SubCommand::with_name("create-queue")
				.about("Create a new queue with a given resource constraint")
				.arg(Arg::with_name("name").index(1).required(true))
				.arg(Arg::with_name("file").long("file").takes_value(true))
				.arg(Arg::with_name("json").long("json").takes_value(true))
				.group(
					ArgGroup::with_name("config")
						.args(&["file", "json"])
						.required(true),
				),
		)
		.subcommand(
			// grass delete-queue default
			SubCommand::with_name("delete-queue")
				.arg(Arg::with_name("name").index(1).required(true))
				// .arg(
				// 	Arg::with_name("paths")
				// 		.index(2)
				// 		.required(true)
				// 		.takes_value(true)
				// 		.multiple(true),
				// )
				// .arg(Arg::with_name("filter").long("filter").takes_value(true)),
		)
		.subcommand(
			// method1. TrailingVarArg: last positional (with or w/o --)
			// method2. AllowExternalSubcommand: cannot use --
			SubCommand::with_name("enqueue")
				.arg(Arg::with_name("name").index(1).required(true))
				.arg(Arg::with_name("cwd").long("cwd").takes_value(true))
				.arg(Arg::with_name("json").long("json").takes_value(true))
				.arg(Arg::with_name("cmd").multiple(true).required(true))
				.arg(
					Arg::with_name("env")
						.short("e")
						.long("env")
						.multiple(true)
						.takes_value(true),
				)
				.setting(AppSettings::TrailingVarArg),
		)
		.subcommand(
			// 1) show
			// 3) show --queue q1
			SubCommand::with_name("show")
				.arg(Arg::with_name("queue").long("queue").takes_value(true)),
		)
		.subcommand(SubCommand::with_name("daemon").setting(AppSettings::Hidden))
}
