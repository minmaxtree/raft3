#[macro_use] extern crate serde_derive;
extern crate raft3;

use raft3::{Command, Reply};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KVCmd {
    Set {
        key: String,
        value: u32,
    },
    Get {
        key: String,
    },
}

#[derive(Serialize, Deserialize)]
pub enum KVRes {
    Succ {
        value: u32,
    },
    Fail {
        msg: String,
    },
    Redire {
        leader_id: usize,
    },
}

impl Command for KVCmd {
}

impl Reply for KVRes {
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
