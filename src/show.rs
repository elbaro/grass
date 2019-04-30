// use std::io;
// use termion::raw::IntoRawMode;
// use termion::input::MouseTerminal;
// use termion::screen::AlternateScreen;
// use tui::Terminal;
// use tui::backend::TermionBackend;
// use tui::widgets::{Widget, Block, Borders};
// use tui::layout::{Layout, Constraint, Direction};

// struct Interactive {
// 	selected: u32,
// 	index: u32,
// }

// impl Interactive {

// }

// pub fn show() -> Result<(), failure::Error> {
// 	let stdout = io::stdout().into_raw_mode()?;
// 	let stdout = MouseTerminal::from(stdout);
// 	let stdout = AlternateScreen::from(stdout);
//     let backend = TermionBackend::new(stdout);
//     let mut terminal = Terminal::new(backend)?;
// 	terminal.hide_cursor()?;

// 	let app = Interactive {selected:0};

// 	loop {
// 		terminal.draw(|mut f| {
// 			let size = f.size();
// 			// Block::default()
// 			//     .title("Block")
// 			//     .borders(Borders::ALL)
// 			//     .render(&mut f, size);

// 			let chunks = Layout::default()
// 				.direction(Direction::Vertical)
// 				.margin(1)
// 				.constraints(
// 					[
// 						Constraint::Percentage(10),
// 						Constraint::Percentage(80),
// 						Constraint::Percentage(10)
// 					].as_ref()
// 				)
// 				.split(f.size());
// 			Block::default()
// 				.title("Block")
// 				.borders(Borders::ALL)
// 				.render(&mut f, chunks[0]);
// 			Block::default()
// 				.title("Block 2")
// 				.borders(Borders::ALL)
// 				.render(&mut f, chunks[2]);


// 		})?;

// 		match events.next()? {
//             Event::Input(key) => match key {
//                 Key::Char('q') => {
//                     break;
//                 }
//                 Key::Down => {
//                     app.index += 1;
//                     if app.index > app.items.len() - 1 {
//                         app.index = 0;
//                     }
//                 }
//                 Key::Up => {
//                     if app.index > 0 {
//                         app.index -= 1;
//                     } else {
//                         app.index = app.items.len() - 1;
//                     }
//                 }
//                 _ => {
// 					println!("unknown key {}", &key);
// 				}
//             },
//             _ => {}
//         };
// 	}
// 	Ok(())
// }
