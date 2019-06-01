#![feature(async_await)]

fn main() -> Result<(), failure::Error>{
	let mut app = tide::App::new();
	app.at("/").get(async move |_| {
		"hello"
	});
	Ok(app.run("127.0.0.1:9999")?)
}
