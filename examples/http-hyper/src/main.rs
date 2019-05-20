extern crate hyper;
extern crate pantomime;

use hyper::rt::Future;
use hyper::service::service_fn_ok;
use hyper::{Body, Request, Response, Server};
use pantomime::dispatcher::*;
use pantomime::prelude::*;
use std::{io, net};

struct MyReaper;

impl Actor<()> for MyReaper {
    fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

    fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
        if let Signal::Started = signal {
            let bind_addr = "127.0.0.1:4567"
                .parse::<net::SocketAddr>()
                .expect("cannot parse address");

            let new_svc = || service_fn_ok(hello_world);

            let server = Server::bind(&bind_addr)
                .executor(ctx.system_context().dispatcher().clone())
                .serve(new_svc)
                .map_err(|e| eprintln!("server error: {}", e));

            ctx.system_context()
                .dispatcher()
                .run(server)
                .expect("cannot run server");
        }
    }
}

/// This example shows how a Tokio program can be run on the Pantomime dispatcher.
///
/// TODO: Show how to integrate with Pantomime actors
fn main() -> io::Result<()> {
    ActorSystem::new().spawn(MyReaper)
}

const PHRASE: &str = "Hello, World!";

fn hello_world(_req: Request<Body>) -> Response<Body> {
    Response::new(Body::from(PHRASE))
}
