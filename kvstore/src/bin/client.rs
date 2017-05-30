extern crate serde_json;
extern crate rustyline;
extern crate kvstore;

use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::net::UdpSocket;
use std::str;
use std::time::{Duration, Instant};
use kvstore::{KVCmd, KVRes};

struct KVClient {
    socket: UdpSocket,
    saddrs: Vec<String>,
}

impl KVClient {
    fn new() -> Self {
        KVClient {
            socket: UdpSocket::bind("127.0.0.1:10000")
                        .expect("couldn't bind"),
            saddrs: (8000..8003)
                        .map(|p| format!("127.0.0.1:{}", p))
                        .collect(),
        }
    }

    fn run(&self) {
        let mut rl = Editor::<()>::new();
        loop {
            let readline = rl.readline(">> ");
            match readline {
                Ok(line) => {
                    let line = line.trim();
                    if !line.is_empty() {
                        println!("Line: {}", line);
                        match parse_line(line) {
                            Ok(kvcmd) => {
                                let timer = Instant::now();
                                let timeout = Duration::from_millis(1000);

                                match self.send_cmd(
                                        &kvcmd, self.saddrs.clone(), 0, timer, timeout) {
                                    Ok(value) => {
                                        println!("value: {}", value);
                                    }
                                    Err(msg) => {
                                        println!("ERR: {}", msg);
                                        break;
                                    }
                                }
                            }
                            Err(msg) => println!("ERR: {}", msg),
                        }
                    }
                },
                Err(ReadlineError::Interrupted) => {
                }
                Err(ReadlineError::Eof) => {
                    break
                },
                Err(err) => {
                    println!("Error: {:?}", err);
                    break
                }
            }
        }
    }

    fn send_cmd(&self, kvcmd: &KVCmd, saddrs: Vec<String>, i: usize,
            timer: Instant, timeout: Duration) -> Result<u32, String> {
        if timer.elapsed() > timeout {
            return Err("timeout".to_owned());
        }

        let saddr = saddrs[i].clone();
        match self.socket.connect(saddr) {
            Ok(_) => {
                let cmd = serde_json::to_string(kvcmd).unwrap();
                match self.socket.send(cmd.as_bytes()) {
                    Ok(_) => {
                        let mut buf = [0; 1024];
                        match self.socket.recv(&mut buf) {
                            Ok(n) => {
                                let res: KVRes = serde_json::from_str(
                                        str::from_utf8(&buf[..n]).unwrap()).unwrap();

                                match res {
                                    KVRes::Redire {
                                        leader_id
                                    } => {
                                        self.send_cmd(kvcmd, saddrs, leader_id, timer, timeout)
                                    }
                                    KVRes::Succ {
                                        value
                                    } => {
                                        Ok(value)
                                    }
                                    KVRes::Fail {
                                        msg
                                    } => {
                                        Err(msg)
                                    }
                                }
                            }
                            Err(_) => {
                                self.send_cmd(kvcmd, saddrs.clone(),
                                    (i + 1) % saddrs.len(), timer, timeout)
                            }
                        }
                    }
                    Err(_) => {
                        self.send_cmd(kvcmd, saddrs.clone(),
                            (i + 1) % saddrs.len(), timer, timeout)
                    }
                }
            }
            Err(_) => {
                self.send_cmd(kvcmd, saddrs.clone(),
                    (i + 1) % saddrs.len(), timer, timeout)
            }
        }
    }
}

fn parse_line(line: &str) -> Result<KVCmd, String> {
    let v: Vec<_> = line.split(' ').collect();

    if v[0] == "get" && v.len() == 2 {
        let key = v[1].to_owned();
        Ok(KVCmd::Get {
            key: key,
        })
    } else if v[0] == "set" && v.len() == 3 {
        let key = v[1].to_owned();
        match v[2].parse::<u32>() {
            Ok(value) => {
                Ok(KVCmd::Set {
                    key: key,
                    value: value,
                })
            }
            Err(_) => {
                Err("Invalid command".to_owned())
            }
        }
    } else {
        Err("Invalid command".to_owned())
    }
}

fn main() {
    let client = KVClient::new();
    client.run();
}
