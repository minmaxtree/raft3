extern crate mio;
extern crate rand;

extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;

use mio::net::UdpSocket;
use mio::{Poll, Events, Ready, PollOpt, Token};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use rand::distributions::{IndependentSample, Range};
use std::str;
use serde::ser::Serialize;
use serde::de::{DeserializeOwned};

const CLUSTER_SIZE: usize = 3;

pub trait Command {
    // fn serialize(&self) -> String;
    // fn deserialize(&str) -> Self;
}

pub trait Reply {
}

pub trait StateMachine {
    type C;
    type R;

    fn apply(&mut self, entry: &Self::C) -> Self::R;

    fn redirect(&self, leader_id: usize) -> Self::R;
}

#[derive(Debug, Eq, PartialEq)]
pub enum State {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry<C> {
    pub command: C,
    pub term: u32,
}

pub struct Raft<S: StateMachine> {
    pub state: State,
    pub term: u32,
    pub votes: usize,
    pub voted_for: Option<usize>,
    pub index: usize,

    pub socket: UdpSocket,
    pub poll: Poll,
    pub events: Events,

    pub timer: Instant,
    pub election_timeout: Duration,

    pub serv_sock: UdpSocket,
    pub log: Vec<Entry<S::C>>,
    pub commit: usize,
    pub last_applied: usize,

    pub next_index: Vec<usize>,
    pub match_index: Vec<usize>,

    pub cluster_size: usize,

    pub client_addr: SocketAddr,

    pub stm: S,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RPC<C> {
    ReqVoteArgs {
        term: u32,
        candidate_id: usize,
        last_log_index: usize,
        last_log_term: u32,
    },
    ReqVoteRes {
        term: u32,
        vote_granted: bool,
    },

    AppendEntArgs {
        term: u32,
        leader_id: usize,
        prev_log_index: usize,
        prev_log_term: u32,
        entries: Vec<Entry<C>>,
        leader_commit: usize,
    },
    AppendEntRes {
        term: u32,
        success: bool,
    }
}

impl<C> RPC<C>
  where C: Command {
    pub fn get_term(&self) -> u32 {
        match *self {
            RPC::ReqVoteArgs { term, .. } => term,
            RPC::ReqVoteRes { term, .. } => term,
            RPC::AppendEntArgs { term, .. } => term,
            RPC::AppendEntRes { term, .. } => term,
        }
    }
}

const SERV_PORT: u16 = 8000;

impl<S> Raft<S>
  where S: StateMachine,
        S::C: Command + Serialize + DeserializeOwned + Clone,
        S::R: Reply + Serialize + DeserializeOwned {
    pub fn new(index: usize, stm: S,) -> Self {
        let mut node = Raft {
            state: State::Follower,
            term: 0,
            // voted: false,
            votes: 0,
            voted_for: None,
            index: index,

            socket: {
                UdpSocket::bind(&address(index)).unwrap()
            },
            poll: {
                Poll::new().unwrap()
            },
            events: Events::with_capacity(64),

            timer: Instant::now(),
            // election_timeout: Duration::from_millis(150),
            election_timeout: gen_rand_to(),
            serv_sock: {
                let serv_addr: SocketAddr =
                        format!("127.0.0.1:{}", SERV_PORT + index as u16).parse().unwrap();
                UdpSocket::bind(&serv_addr).unwrap()
            },
            log: Vec::new(),
            commit: 0,
            last_applied: 0,

            next_index: vec![1; CLUSTER_SIZE as usize],
            match_index: vec![0; CLUSTER_SIZE as usize],

            cluster_size: CLUSTER_SIZE,

            client_addr: "0.0.0.0:0".parse().unwrap(),

            // leader_id: 0,

            stm: stm,
        };

        node.poll.register(&node.socket, Token(0),
            Ready::readable() | Ready::writable(),
            PollOpt::edge()).unwrap();

        node.serv_client();

        node
    }

    pub fn send(&self, addr: &SocketAddr, buf: &[u8]) -> usize {
        self.socket.send_to(buf, addr).unwrap()
    }

    pub fn send_peer(&self, index: usize, buf: &[u8]) -> usize {
        match self.socket.send_to(buf, &address(index)) {
            Ok(n) => n,
            _ => 0,
        }
    }

    pub fn send_peers(&self, buf: &[u8]) {
        for i in (0..self.cluster_size).filter(|&i| i != self.index) {
            self.send_peer(i, buf);
        }
    }

    pub fn send_heartbeat(&self) {
        for i in (0..self.cluster_size).filter(|&i| i != self.index) {
            self.replicate_log(i);
        }
    }

    pub fn send_rpc(&self, index: usize, rpc: RPC<S::C>) {
        let rpc = serde_json::to_string(&rpc).unwrap();
        self.send_peer(index, rpc.as_bytes());
    }

    pub fn req_vote(&self) {
        let req_vote_args: RPC<S::C> = RPC::ReqVoteArgs {
            term: self.term,
            candidate_id: self.index,
            last_log_index: self.log.len(),
            last_log_term: self.log.last().map_or(0, |e| e.term),
        };
        let buf = serde_json::to_string(&req_vote_args).unwrap();
        self.send_peers(buf.as_bytes());
    }

    pub fn recv_rpc(&mut self) -> (RPC<S::C>, usize) {
        let mut buf = [0; 1024];
        let (n ,addr) = self.socket.recv_from(&mut buf).unwrap();
        let rpc = serde_json::from_str(str::from_utf8(&buf[0..n]).unwrap()).unwrap();
        let id = addr.port() - 9000;
        (rpc, id as usize)
    }

    pub fn reply_append_ent(&self, leader_id: usize, success: bool) {
        let reply: RPC<S::C> = RPC::AppendEntRes {
            term: self.term,
            success: success,
        };

        let j = serde_json::to_string(&reply).unwrap();
        self.send_peer(leader_id, j.as_bytes());
    }

    pub fn reset_timer(&mut self) {
        self.timer = Instant::now();
    }

    pub fn timeout(&self) -> Option<Duration> {
        let elapsed = self.timer.elapsed();
        if elapsed > self.election_timeout {
            None
        } else {
            Some(self.election_timeout - elapsed)
        }
    }

    pub fn start_election(&mut self) {
        self.state = State::Candidate;
        self.term += 1;

        self.votes = 1;
        self.voted_for = Some(self.index);
        // self.voted = true;

        self.req_vote();

        self.reset_timer();

        let mut rng = rand::thread_rng();
        let between = Range::new(200, 400);
        let election_timeout = between.ind_sample(&mut rng);
        self.election_timeout = Duration::from_millis(election_timeout);
    }

    pub fn as_follower(&mut self, term: u32, id: usize) {
        self.term = term;
        self.state = State::Follower;
        self.votes = 0;
        // self.voted_for = None;
        self.voted_for = Some(id);
        // self.voted = false;
        // self.timer = Instant::now();
    }

    pub fn as_leader(&mut self) {
        self.state = State::Leader;
        self.send_heartbeat();
        self.timer = Instant::now();
        self.election_timeout = Duration::from_millis(150);

        self.next_index = vec![self.log.len() + 1; self.cluster_size];
        self.match_index = vec![0; self.cluster_size];
    }

    pub fn serv_client(&mut self) {
        let ready = Ready::readable() | Ready::writable();
        self.poll.register(&self.serv_sock, Token(1), ready, PollOpt::edge()).unwrap();
    }

    pub fn apply_log(&mut self, log_index: usize) -> S::R {
        self.stm.apply(&((self.log[log_index - 1]).command))
    }

    pub fn apply_logs(&mut self) {
        for i in self.last_applied..self.commit {
            if i >= 1 {
                self.stm.apply(&((self.log[i - 1]).command));
            }
        }
    }

    pub fn reply_client(&self, reply: S::R) {
        let reply = serde_json::to_string(&reply).unwrap();
        self.serv_sock.send_to(reply.as_bytes(), &self.client_addr).unwrap();
    }

    pub fn replicate_log(&self, id: usize) {
        println!("self.next_index is {:?}", self.next_index);
        let prev_log_index = self.next_index[id] - 1;
        println!("prev_log_index is {}, self.log.len() is {}",
            prev_log_index, self.log.len());
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            self.log[prev_log_index - 1].term
        };

        let entries = self.log[prev_log_index..].to_vec();

        let append_ent_args: RPC<S::C> = RPC::AppendEntArgs {
            term: self.term,
            leader_id: self.index,
            prev_log_term: prev_log_term,
            prev_log_index: prev_log_index,
            entries: entries,
            leader_commit: self.commit,
        };

        let j = serde_json::to_string(&append_ent_args).unwrap();

        self.send_peer(id, j.as_bytes());
    }

    pub fn add_to_log(&mut self, cmd: S::C) {
        let entry = Entry {
            command: cmd,
            term: self.term,
        };
        self.log.push(entry);
    }

    pub fn update_log(&mut self, prev_log_index: usize, entries: Vec<Entry<S::C>>) {
        self.log.split_off(prev_log_index);
        self.log.extend(entries);
    }

    pub fn has_entry(&self, prev_log_index: usize, prev_log_term: u32) -> bool {
        if prev_log_index == 0 {
            true
        } else if prev_log_index > self.log.len() {
            false
        } else {
            self.log[prev_log_index - 1].term == prev_log_term
        }
    }

    pub fn redire_cli(&self) {
        if let Some(leader_id) = self.voted_for {
            let res = self.stm.redirect(leader_id);
            let j = serde_json::to_string(&res).unwrap();
            self.serv_sock.send_to(j.as_bytes(), &self.client_addr).unwrap();
        }
    }

    fn vote_req_up_to_date(&self, last_log_index: usize, last_log_term: u32) -> bool {
        let self_last_log_term = self.log.last().map_or(0, |e| e.term);
        if self_last_log_term > last_log_term {
            false
        } else if self_last_log_term < last_log_term {
            true
        } else if self.log.len() <= last_log_index {
            true
        } else {
            false
        }
    }

    pub fn start(&mut self) {
        self.reset_timer();
        let mut events = Events::with_capacity(64);
        loop {
            if self.timeout() == None {
                if self.state == State::Leader {
                    self.send_heartbeat();
                    self.reset_timer();
                } else {
                    self.start_election();
                }
            } else {
                let poll_timeout = self.timeout();
                self.poll.poll(&mut events, poll_timeout).unwrap();
                for event in events.iter() {
                    if event.token() == Token(0)
                            && event.readiness().is_readable() {
                        let (rpc, id) = self.recv_rpc();

                        let rpc_term = rpc.get_term();
                        if rpc_term > self.term {
                            self.as_follower(rpc_term, id);
                        }

                        match rpc {
                            RPC::ReqVoteArgs {
                                term,
                                candidate_id,
                                last_log_index,
                                last_log_term,
                            } => {
                                let mut vote_granted = false;
                                if term == self.term &&
                                        (self.voted_for == None || 
                                            self.voted_for == Some(candidate_id)) &&
                                        self.vote_req_up_to_date(last_log_index, last_log_term) {
                                    vote_granted = true;
                                    self.voted_for = Some(candidate_id);
                                }

                                let req_vote_res = RPC::ReqVoteRes {
                                    term: term,
                                    vote_granted: vote_granted
                                };
                                self.send_rpc(candidate_id, req_vote_res);

                                if vote_granted {
                                    self.reset_timer();
                                }
                            }
                            RPC::ReqVoteRes { term, vote_granted }
                                    if self.state == State::Candidate => {
                                if term == self.term {
                                    if vote_granted {
                                        self.votes += 1;
                                    }
                                    if self.votes > self.cluster_size / 2 {
                                        println!("becomes leader");
                                        self.as_leader();
                                        // self.serv_client();
                                    }
                                }
                            }
                            RPC::AppendEntArgs {
                                term,
                                leader_id,
                                prev_log_index,
                                prev_log_term,
                                entries,
                                leader_commit,
                            } => {
                                if Some(leader_id) == self.voted_for {
                                    if term == self.term {
                                        self.reset_timer();
                                    }
                                }

                                if term < self.term ||
                                        !self.has_entry(prev_log_index, prev_log_term) {
                                    self.reply_append_ent(leader_id, false);
                                } else {
                                    println!("[1] self.commit is {}, leader_commit is {}",
                                        self.commit, leader_commit);
                                    self.update_log(prev_log_index, entries);
                                    if leader_commit > self.commit {
                                        let last_ent_index = self.log.len();
                                        if last_ent_index < self.commit {
                                            self.commit = last_ent_index;
                                        } else {
                                            self.commit = leader_commit;
                                        }

                                        println!("[follower] self.commit is {}, self.last_applied is {}",
                                            self.commit, self.last_applied);
                                        if self.commit > self.last_applied {
                                            // self.last_applied += 1;
                                            // let log_index = self.last_applied;
                                            // self.apply_log(log_index);
                                            self.apply_logs();
                                            self.last_applied = self.commit;
                                        }
                                    }
                                    self.reply_append_ent(leader_id, true);

                                    if self.state == State::Candidate {
                                        if self.term == term {
                                            // self.leader_id = leader_id;
                                            // self.voted_for = Some(leader_id);
                                            self.as_follower(term, leader_id);
                                            // self.voted_for = Some(leader_id);
                                        }
                                    }
                                }
                            }
                            RPC::AppendEntRes {
                                term,
                                success,
                            } if self.state == State::Leader => {
                                if self.term == term {
                                    if success {
                                        self.match_index[id] = self.log.len();
                                        self.next_index[id] = self.log.len() + 1;
                                    } else {
                                        if self.next_index[id] > 1 {
                                            self.next_index[id] -= 1;
                                        }
                                        self.replicate_log(id);
                                    }

                                    let match_index = self.match_index.clone();
                                    let s: Vec<_> = match_index.iter()
                                            .filter(|&&i| i > self.commit).collect();
                                    if s.len() + 1 > self.cluster_size / 2 {
                                        self.commit = **s.iter().min().unwrap();
                                        if self.commit > self.last_applied {
                                            self.last_applied += 1;
                                            let log_index = self.last_applied;
                                            println!("[server] self.commit is {}", self.commit);
                                            let reply = self.apply_log(log_index);
                                            self.reply_client(reply);
                                        }
                                    }
                                }
                            }
                            _ => {
                            }
                        }
                    } else if event.token() == Token(1) && event.readiness().is_readable() {
                        let mut buf = [0; 1024];
                        let (n, client_addr) = self.serv_sock.recv_from(&mut buf).unwrap();
                        self.client_addr = client_addr;
                        println!("received {} bytes from client: {}",
                            n, str::from_utf8(&buf).unwrap());

                        if self.state == State::Leader {
                            let cmd: S::C = serde_json::from_str(
                                str::from_utf8(&buf[..n]).unwrap()).unwrap();
                            self.add_to_log(cmd);
                            for i in (0..self.cluster_size).filter(|&i| i != self.index) {
                                if self.log.len() + 1 > self.next_index[i as usize] {
                                    self.replicate_log(i);
                                }
                            }
                        } else {
                            self.redire_cli();
                        }
                    }
                }
            }
        }
    }
}

pub fn address(index: usize) -> SocketAddr {
    format!("127.0.0.1:{}", index + 9000).parse().unwrap()
}

fn gen_rand_to() -> Duration {
    let mut rng = rand::thread_rng();
    let between = Range::new(200, 400);
    let timeout = between.ind_sample(&mut rng);
    Duration::from_millis(timeout)
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
