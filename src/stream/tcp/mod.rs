use crate::stream::{Source, Sink};
use std::net::SocketAddr;

struct TcpConnection {
    source: Source<Vec<u8>>,
    sink: Sink<Vec<u8>, ()>
}

struct Tcp;

impl Tcp {
    fn bind(addr: &SocketAddr) -> Source<TcpConnection> {
        unimplemented!()
    }

    fn connect(addr: &SocketAddr) -> TcpConnection {
        unimplemented!()
    }
}
