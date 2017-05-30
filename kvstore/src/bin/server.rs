extern crate raft3;
extern crate kvstore;

use raft3::{StateMachine, Raft};
use std::collections::HashMap;
use std::env::args;
use std::str;
use kvstore::{KVCmd, KVRes};

struct KVEngine {
    dict: HashMap<String, u32>,
}

impl KVEngine {
    fn new() -> KVEngine {
        KVEngine {
            dict: HashMap::new(),
        }
    }
}

impl StateMachine for KVEngine {
    type C = KVCmd;
    type R = KVRes;

    fn apply(&mut self, command: &Self::C) -> Self::R {
        let command = command.clone();
        match command {
            KVCmd::Set {
                key,
                value
            } => {
                self.dict.insert(key, value);
                KVRes::Succ { value: value }
            }
            KVCmd::Get {
                key,
            } => {
                match self.dict.get(&key) {
                    Some(value) => {
                        KVRes::Succ { value: *value }
                    }
                    None => {
                        KVRes::Fail { msg: "Not Found".to_owned() }
                    }
                }
            }
        }
    }

    fn redirect(&self, leader_id: usize) -> Self::R {
        KVRes::Redire { leader_id: leader_id }
    }
}

fn main() {
    let index: usize = args().nth(1).expect("Need one argument")
        .parse().unwrap();
    let stm = KVEngine::new();
    let mut node = Raft::new(index, stm);
    println!("socket.local_addr is {:?}", node.socket.local_addr());

    node.start();
}
