use futures::compat::Compat;
use futures::{Future, FutureExt};
use futures::compat::Executor01CompatExt;

pub fn tokio_run<F: Future<Output = ()> + Send + 'static>(future: F) {
	tarpc::init(tokio::executor::DefaultExecutor::current().compat());
	tokio::run(Compat::new(Box::pin(
		future.map(|()| -> Result<(), ()> { Ok(()) }),
	)));
}

pub fn tokio_spawn<F: Future<Output = ()> + Send + 'static>(future: F) {
	tokio::spawn(Compat::new(Box::pin(
		future.map(|()| -> Result<(), ()> { Ok(()) }),
	)));
}

// use tokio::prelude::*;
// use std::net::SocketAddr;
// use futures::StreamExt;
// pub fn tarpc_server_session<T,A,B>(conn: T) -> tarpc::server::Channel<A,B,tarpc_bincode_transport::Transport<T,A,B>>
// where T:AsyncRead+AsyncWrite {
// 	let transport = tarpc_bincode_transport::new(conn);//.fuse(); // fuse from Future03 ext trait
// 	let transport = transport.fuse();
// 	let (sender, _recv) = futures::channel::mpsc::unbounded::<SocketAddr>();
// 	let channel = tarpc::server::Channel::new_simple_channel(transport, sender);
// 	return channel;
// }
