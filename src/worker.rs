use crate::rpc::Foo;
use essrpc::RPCClient;

pub fn run() {
	let stream = std::os::unix::net::UnixStream::connect("/tmp/broker.sock").unwrap();

	println!("connected");
	let mut client =
		crate::rpc::FooRPCClient::new(essrpc::transports::BincodeTransport::new(stream));
	println!("{:?}", client.job_update());
	println!("{:?}", client.worker_introduce());
	println!("done");
}
