extern crate pantomime;

use pantomime::prelude::*;
use std::{io, process};

type Uuid = (u64, u64); // @TODO this is (higher, lower) variants, but probably better to find a community-sanctioned type
type NodeId = Uuid;

struct Heartbeat {}

enum ServerMsg {
    Heartbeat,
    HeartbeatTimeout,
    RequestVote(u64, ActorRef<ServerMsg>),
    VoteResponse(bool)
}

enum State {Leader, Follower, Candidate { votes: u64 } }

struct Server {
    state: State,
    current_term: u64,
    current_term_voted: bool,
    voted_for: Option<NodeId>
}

impl Actor<ServerMsg> for Server {
    fn receive(&mut self, msg: ServerMsg, ctx: &mut ActorContext<ServerMsg>) {
        match self.state {
            State::Leader => {
                self.leader_receive(msg, ctx);
            }

            State::Follower => {
                self.follower_receive(msg, ctx);
            }

            State::Candidate { mut votes } => {
                self.candidate_receive(&mut votes, msg, ctx);
            }
        }
    }
}

impl Server {
    fn change_term(&mut self, to: u64) {
        self.current_term = to;
        self.current_term_voted = false;
    }

    fn vote_for_term(&mut self, to: ActorRef<ServerMsg>, response: bool) {
        if !self.current_term_voted {
            self.current_term_voted = true;
            to.tell(ServerMsg::VoteResponse(false));
        }
    }

    /// Sends the supplied message to everyone.
    fn tell_all(&mut self, msg: ServerMsg) {
        unimplemented!()
    }

    fn tell_once(&mut self, node: usize, msg: ServerMsg) {
        unimplemented!()
    }

    fn leader_receive(&mut self, msg: ServerMsg, ctx: &mut ActorContext<ServerMsg>) {
        match msg {
            ServerMsg::RequestVote(term, reply_to) => {
                // we're the leader, not going to support this coup

                reply_to.tell(ServerMsg::VoteResponse(false));
            }

            ServerMsg::Heartbeat => {}

            ServerMsg::HeartbeatTimeout => {}

            ServerMsg::VoteResponse(_) => {}
        }
    }

    fn follower_receive(&mut self, msg: ServerMsg, ctx: &mut ActorContext<ServerMsg>) {
        match msg {
            ServerMsg::RequestVote(term, reply_to) if term < self.current_term => {
                reply_to.tell(ServerMsg::VoteResponse(false));
            }

            ServerMsg::RequestVote(term, reply_to) if term == self.current_term => {

            }

            ServerMsg::RequestVote(term, reply_to) => {
            }

            ServerMsg::Heartbeat => {
            }

            ServerMsg::HeartbeatTimeout => {
                self.state = State::Candidate { votes: 1 }; // 1 vote because we vote for ourselves
                self.change_term(self.current_term + 1);
                self.tell_all(ServerMsg::RequestVote(self.current_term, ctx.actor_ref().clone()));
            }

            ServerMsg::VoteResponse(_) => {}
        }
    }

    fn candidate_receive(&mut self, votes: &mut u64, msg: ServerMsg, ctx: &mut ActorContext<ServerMsg>) {
        match msg {
            ServerMsg::RequestVote(term, reply_to) if term > self.current_term => {
                
            }

            ServerMsg::RequestVote(_, _) => {
                // not voting - we have a better term
            }

            ServerMsg::Heartbeat => {

            }

            ServerMsg::HeartbeatTimeout => {

            }

            ServerMsg::VoteResponse(_) => {}
        }
    }
}

struct Term {
    id: u64
}

struct Reaper;

impl Actor<()> for Reaper {
    fn receive(&mut self, _: (), _: &mut ActorContext<()>) {}

    fn receive_signal(&mut self, signal: Signal, ctx: &mut ActorContext<()>) {
        if let Signal::Started = signal {
        }
    }
}

fn main() -> io::Result<()> {
    ActorSystem::new().spawn(Reaper)
}
