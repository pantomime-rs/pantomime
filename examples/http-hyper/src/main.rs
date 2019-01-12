extern crate hyper;
extern crate pantomime;

use hyper::{Body, Request, Response, Server};
use hyper::rt::Future;
use hyper::service::service_fn_ok;
use pantomime::prelude::*;
use pantomime::dispatcher::*;
use std::{io, net};


/// This example shows how a Tokio program can be run on the Pantomime dispatcher.
///
/// TODO: Show how to integrate with Pantomime actors
fn main() -> io::Result<()> {
    let system = ActorSystem::new().start();

    let bind_addr = "127.0.0.1:4567".parse::<net::SocketAddr>().expect("cannot parse address");

    let new_svc = || {
        service_fn_ok(hello_world)
    };

    let server = Server::bind(&bind_addr)
        .executor(system.context.dispatcher.clone())
        .serve(new_svc)
        .map_err(|e| eprintln!("server error: {}", e));

    system.context.dispatcher.run(server)?;

    system.join();

    Ok(())
}

const PHRASE: &str = "Hello, World!";

fn hello_world(_req: Request<Body>) -> Response<Body> {
    Response::new(Body::from(PHRASE))
}

