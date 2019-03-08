use clap::{App, AppSettings, Arg, SubCommand};
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
			SubCommand::with_name("create-queue")
				.about("Create a new queue with a given resource constraint")
				.arg(Arg::with_name("queue").index(1).required(true))
				.arg(Arg::with_name("file").long("file").takes_value(true))
				.arg(Arg::with_name("cpu").long("cpu").takes_value(true))
				.arg(Arg::with_name("gpu").long("gpu").takes_value(true)),
		)
		.subcommand(
			SubCommand::with_name("delete-queue")
				.arg(Arg::with_name("queue").index(1).required(true))
				.arg(
					Arg::with_name("paths")
						.index(2)
						.required(true)
						.takes_value(true)
						.multiple(true),
				)
				.arg(Arg::with_name("filter").long("filter").takes_value(true)),
		)
		.subcommand(
			// psutil eval ./a.out data/A/*.in data/B/*.out --time 0.5 --memory 64
			SubCommand::with_name("enqueue")
				.arg(Arg::with_name("cwd").long("cwd").takes_value(true))
				.arg(Arg::with_name("cmd").multiple(true))
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
