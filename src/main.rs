// extern crate mio;
// extern crate rand;
// extern crate serde;
// extern crate serde_json;

// extern crate raft3;

// use std::env::args;
// use mio::{Events, Token};
// use std::str;

// use raft3::{State, Raft, RPC, KVCmd};

// fn main() {
//     let index: usize = args().nth(1).expect("Need one argument")
//         .parse().unwrap();
//     let mut node = Raft::new(index);
//     println!("socket.local_addr is {:?}", node.socket.local_addr());

//     node.reset_timer();
//     let mut events = Events::with_capacity(64);
//     loop {
//         if node.timeout() == None {
//             if node.state == State::Leader {
//                 node.send_heartbeat();
//                 node.reset_timer();
//             } else {
//                 node.start_election();
//             }
//         } else {
//             let poll_timeout = node.timeout();
//             node.poll.poll(&mut events, poll_timeout).unwrap();
//             for event in events.iter() {
//                 if event.token() == Token(0)
//                         && event.readiness().is_readable() {
//                     let (rpc, id) = node.recv_rpc();

//                     let rpc_term = rpc.get_term();
//                     if rpc_term > node.term {
//                         node.as_follower(rpc_term, id);
//                     }

//                     match rpc {
//                         RPC::ReqVoteArgs { term, candidate_id, .. } => {
//                             let mut vote_granted = false;
//                             if term == node.term &&
//                                     (node.voted_for == None || 
//                                         node.voted_for == Some(candidate_id)) {
//                                 vote_granted = true;
//                                 node.voted_for = Some(candidate_id);
//                             }

//                             let req_vote_res = RPC::ReqVoteRes {
//                                 term: term,
//                                 vote_granted: vote_granted
//                             };
//                             node.send_rpc(candidate_id, req_vote_res);

//                             if vote_granted {
//                                 node.reset_timer();
//                             }
//                         }
//                         RPC::ReqVoteRes { term, vote_granted }
//                                 if node.state == State::Candidate => {
//                             if term == node.term {
//                                 if vote_granted {
//                                     node.votes += 1;
//                                 }
//                                 if node.votes > node.cluster_size / 2 {
//                                     println!("becomes leader");
//                                     node.as_leader();
//                                     node.serv_client();
//                                 }
//                             }
//                         }
//                         RPC::AppendEntArgs {
//                             term,
//                             leader_id,
//                             prev_log_index,
//                             prev_log_term,
//                             entries,
//                             leader_commit,
//                         } => {
//                             if Some(leader_id) == node.voted_for {
//                                 if term == node.term {
//                                     println!("reset timer, node.election_timeout is {:?}",
//                                         node.election_timeout);
//                                     node.reset_timer();
//                                 }
//                             }

//                             if term < node.term ||
//                                     !node.has_entry(prev_log_index, prev_log_term) {
//                                 node.reply_append_ent(leader_id, false);
//                             } else {
//                                 node.update_log(prev_log_index, entries);
//                                 if leader_commit > node.commit {
//                                     node.commit = leader_commit;
//                                     let last_ent_index = node.log.len() - 1;
//                                     if last_ent_index < node.commit {
//                                         node.commit = last_ent_index;
//                                     }

//                                     if node.commit > node.last_applied {
//                                         node.last_applied += 1;
//                                         let log_index = node.last_applied;
//                                         node.apply_log(log_index);
//                                     }
//                                 }
//                                 node.reply_append_ent(leader_id, true);

//                                 if node.state == State::Candidate {
//                                     if node.term == term {
//                                         // node.leader_id = leader_id;
//                                         // node.voted_for = Some(leader_id);
//                                         node.as_follower(term, leader_id);
//                                         // node.voted_for = Some(leader_id);
//                                     }
//                                 }
//                             }
//                         }
//                         RPC::AppendEntRes {
//                             term,
//                             success,
//                         } if node.state == State::Leader => {
//                             if node.term == term {
//                                 if success {
//                                     node.match_index[id] = node.log.len();
//                                     node.next_index[id] += node.log.len() + 1;
//                                 } else {
//                                     node.next_index[id] -= 1;
//                                     node.replicate_log(id);
//                                 }

//                                 let match_index = node.match_index.clone();
//                                 let s: Vec<_> = match_index.iter()
//                                         .filter(|&&i| i > node.commit).collect();
//                                 if s.len() + 1 > node.cluster_size / 2 {
//                                     node.commit = **s.iter().min().unwrap();
//                                     if node.commit > node.last_applied {
//                                         node.last_applied += 1;
//                                         let log_index = node.last_applied;
//                                         let reply = node.apply_log(log_index);
//                                         node.reply_client(reply);
//                                     }
//                                 }
//                             }
//                         }
//                         _ => {
//                         }
//                     }
//                 } else if event.token() == Token(1) && event.readiness().is_readable() {
//                     let mut buf = [0; 1024];
//                     let (n, client_addr) = node.serv_sock.recv_from(&mut buf).unwrap();
//                     node.client_addr = client_addr;
//                     println!("received {} bytes from client: {}",
//                         n, str::from_utf8(&buf).unwrap());

//                     if node.state == State::Leader {
//                         let cmd: KVCmd = serde_json::from_str(
//                             str::from_utf8(&buf[..n]).unwrap()).unwrap();
//                         node.add_to_log(cmd);
//                         for i in (0..node.cluster_size).filter(|&i| i != node.index) {
//                             if node.log.len() + 1 > node.next_index[i as usize] as usize {
//                                 node.replicate_log(i);
//                             }
//                         }
//                     } else {
//                         node.redire_cli();
//                     }
//                 }
//             }
//         }
//     }
// }

fn main() {}
