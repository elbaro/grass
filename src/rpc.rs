use crate::objects::{Job, JobSpecification, JobStatus, WorkerCapacity, WorkerInfo};

// example

// let stream = tokio::net::UnixStream::connect("/tmp/broker.sock").unwrap();
// let mut client = crate::rpc::BrokerRPCClient::new(essrpc::transports::BincodeTransport::new(stream));

// impl crate::rpc::Broker for BrokerRPCServerImpl
// let listener = std::net::TcpListener::bind(&self.bind_addr).unwrap();
// let mut s = crate::rpc::BrokerRPCServer::new(
// 	BrokerRPCServerImpl{broker:&self},
// 	essrpc::transports::BincodeTransport::new(stream),
// );
// s.serve();

// #[essrpc::essrpc(sync)]
pub trait Broker {
	/// Broker <-> Worker
	// type JobUpdateRet = Ready<()>;
	// type JobRequesteRet = Ready<Option<Job>>;
	// type WorkerIntroduceRet = Ready<()>;
	// type WorkerHeartbeatRet = Ready<()>;

	fn job_update(&self, job_id: String, status: JobStatus) -> Result<String, essrpc::RPCError>;
	fn job_request(&self, capacity: WorkerCapacity) -> Result<Option<Job>, essrpc::RPCError>;
	fn worker_introduce(&self) -> Result<String, essrpc::RPCError>;
	fn worker_heartbeat(&self) -> Result<String, essrpc::RPCError>;

	/// Broker <-> CLI
	fn job_enqueue(&self, spec: JobSpecification) -> Result<(), essrpc::RPCError>;
	fn show(&self) -> Result<String, essrpc::RPCError>;
}

/// Daemon <-> CLI RPC
// #[essrpc::essrpc(sync, async)]
pub trait Daemon {
	fn stop(&self) -> Result<(), essrpc::RPCError>;
}
