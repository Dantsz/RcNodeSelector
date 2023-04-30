use arrayref::array_ref;
use clap::Parser;
use crossbeam::channel::{unbounded, Receiver, SendError};
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use std::net::UdpSocket;
use std::thread::{self};
use std::time::Duration;

#[derive(Serialize, Deserialize, Parser, Debug)]
struct Args {
    #[arg(default_value_t = 1111)]
    port: i64,
}

enum ThreadAction {
    STOP,
}

trait Node {
    fn listen(&self) -> std::io::Result<()>;
}

struct ChildNode {
    ip_address: String,
    divisor_value: usize,
    parent_connection: Receiver<ThreadAction>,
}

impl Node for ChildNode {
    fn listen(&self) -> std::io::Result<()> {
        let socket = UdpSocket::bind(&self.ip_address)?;
        let timeout = Duration::from_millis(500);
        socket.set_read_timeout(Some(timeout))?;
        const USIZE_SIZE: usize = size_of::<usize>();
        let mut buf = vec![0; USIZE_SIZE];
        loop {
            match socket.recv_from(&mut buf) {
                Ok((size, src)) => {
                    let received = &buf[..size];
                    let received_usize = usize::from_le_bytes(*array_ref!(received, 0, USIZE_SIZE));
                    println!(
                        "{} : Received {} from {}",
                        self.ip_address, received_usize, src
                    );
                    if received_usize % self.divisor_value == 0 {
                        println!(
                            "{} Found {} which is divisible by {} , sending ACK  to master",
                            self.ip_address, received_usize, self.divisor_value
                        );
                        socket.send_to("ACK".as_bytes(), src)?;
                    }
                }
                Err(ref err) if err.kind() == std::io::ErrorKind::TimedOut => {
                    if !self.parent_connection.is_empty() {
                        return Ok(());
                    }
                }
                Err(_) => {}
            }
        }
    }
}

struct MasterNodeData {
    ip_address: String,
    stop_connection: Receiver<ThreadAction>,
    listener_nodes: Vec<String>,
}

impl Node for MasterNodeData {
    fn listen(&self) -> std::io::Result<()> {
        let socket = UdpSocket::bind(&self.ip_address)?;
        let timeout = Duration::from_millis(500);
        let mut buf = vec![0; 3];
        socket.set_read_timeout(Some(timeout))?;
        for i in 0usize..100usize {
            for addr in &self.listener_nodes {
                socket.send_to(&i.to_ne_bytes(), addr)?;
            }
        }
        loop {
            match socket.recv_from(&mut buf) {
                Ok((size, src)) => {
                    let received = String::from_utf8(buf[..size].to_vec()).expect("");
                    println!("{} : Received {} from {}", self.ip_address, received, src);
                }
                Err(ref err) if err.kind() == std::io::ErrorKind::TimedOut => {
                    if !self.stop_connection.is_empty() {
                        return Ok(());
                    }
                }
                Err(_) => {}
            }
        }
    }
}

fn main() -> Result<(), SendError<ThreadAction>> {
    let args = Args::parse();
    let node1_address = format!("127.0.0.4:{}", args.port);
    let node2_address = format!("127.0.0.2:{}", args.port);
    let node3_address = format!("127.0.0.3:{}", args.port);

    println!("Creating channels");
    let (n1_tx, n1_rx) = unbounded::<ThreadAction>();
    let (n2_tx, n2_rx) = unbounded::<ThreadAction>();
    let (n3_tx, n3_rx) = unbounded::<ThreadAction>();
    let (_f_tx, _f_rx) = unbounded::<ThreadAction>();
    println!("Starting listener threads");
    let node2_data = ChildNode {
        divisor_value: 3,
        ip_address: node2_address.to_owned(),
        parent_connection: n2_rx,
    };
    let node3_data = ChildNode {
        divisor_value: 5,
        ip_address: node3_address.to_owned(),
        parent_connection: n3_rx,
    };
    let _node2_handler = thread::spawn(move || node2_data.listen());
    let _node3_handler = thread::spawn(move || node3_data.listen());
    println!("Starting master node");
    let _node1_handler = thread::spawn(move || {
        MasterNodeData {
            ip_address: node1_address.to_owned(),
            stop_connection: n1_rx,
            listener_nodes: vec![node2_address.to_owned(), node3_address.to_owned()],
        }
        .listen()
    });
    thread::sleep(Duration::from_secs(5));
    println!("Times out, sending kill signals to threads");
    n2_tx.send(ThreadAction::STOP)?;
    n3_tx.send(ThreadAction::STOP)?;
    n1_tx.send(ThreadAction::STOP)?;
    println!("END");
    Ok(())
}
