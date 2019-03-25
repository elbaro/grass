use essrpc::RPCServer;

struct Broker {}

impl crate::rpc::Foo for Broker {
	fn job_update(&self) -> Result<String, essrpc::RPCError> {
		println!("     up");
		std::thread::sleep_ms(4000);
		Ok("update".into())
	}
	fn job_request(&self) -> Result<String, essrpc::RPCError> {
		println!("     req");
		Ok("req".into())
	}
	fn worker_introduce(&self) -> Result<String, essrpc::RPCError> {
		println!("     intro");
		Ok("intro".into())
	}
	fn worker_heartbeat(&self) -> Result<String, essrpc::RPCError> {
		println!("     heart");
		std::thread::sleep_ms(2000);
		Ok("heartbeat".into())
	}
}

pub fn run() {
	let listener = std::os::unix::net::UnixListener::bind("/tmp/broker.sock").unwrap();
	for stream in listener.incoming() {
		match stream {
			Ok(stream) => {
				println!("connected");
				let mut s = crate::rpc::FooRPCServer::new(
					Broker {},
					essrpc::transports::BincodeTransport::new(stream),
				);
				s.serve();
				println!("served");
			}
			Err(err) => {
				break;
			}
		}
	}
}
