
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





// #[essrpc::essrpc(sync, async)]
