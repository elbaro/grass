#[essrpc::essrpc(sync, async)]
pub trait Foo {
	fn job_update(&self) -> Result<String, essrpc::RPCError>;
	fn job_request(&self) -> Result<String, essrpc::RPCError>;
	fn worker_introduce(&self) -> Result<String, essrpc::RPCError>;
	fn worker_heartbeat(&self) -> Result<String, essrpc::RPCError>;
}
