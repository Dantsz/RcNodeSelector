use arrayref::array_ref;
use clap::Parser;
use crossbeam::channel::{unbounded, Receiver, SendError};
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use std::net::UdpSocket;
use std::thread::{self, Thread};
use std::time::Duration;

#[derive(Serialize, Deserialize, Parser, Debug)]
struct Args {
    #[arg(default_value_t = 1111)]
    port: i64,
}

enum ThreadAction {
    STARTEDLISTENING,
    STOP,
}

struct ChildNode {
    ip_adress: String,
    divisor_value: usize,
    parent_connection: Receiver<ThreadAction>,
}

fn listener_node_routine(data: ChildNode) -> std::io::Result<()> {
    let socket = UdpSocket::bind(&data.ip_adress)?;
    let timeout = Duration::from_millis(500);
    socket.set_read_timeout(Some(timeout))?;
    const USIZE_SIZE: usize = size_of::<usize>();
    let mut buf = vec![0; USIZE_SIZE];
    'listen_loop: loop {
        match socket.recv_from(&mut buf) {
            Ok((size, src)) => {
                let received = &buf[..size];
                let received_usize = usize::from_le_bytes(*array_ref!(received, 0, USIZE_SIZE));
                println!(
                    "{} : Received {} from {}",
                    data.ip_adress, received_usize, src
                );
                if received_usize % data.divisor_value == 0 {
                    println!(
                        "{} Found {} which is divisible by {} , sending ACK  to master",
                        data.ip_adress, received_usize, data.divisor_value
                    );
                    socket.send_to("ACK".as_bytes(), src)?;
                }
            }
            Err(ref err) if err.kind() == std::io::ErrorKind::TimedOut => {
                if !data.parent_connection.is_empty() {
                    return Ok(());
                }
            }
            Err(_) => {}
        }
    }
    Ok(())
}

struct MasterNodeData {
    ip_adress: String,
    stop_connection: Receiver<ThreadAction>,
    listener_nodes: Vec<String>,
}
fn master_node_routine(data: MasterNodeData) -> std::io::Result<()> {
    let socket = UdpSocket::bind(&data.ip_adress)?;
    let timeout = Duration::from_millis(500);
    let mut buf = vec![0; 3];
    socket.set_read_timeout(Some(timeout))?;
    for i in 0usize..100usize {
        for addr in &data.listener_nodes {
            socket.send_to(&i.to_ne_bytes(), addr)?;
        }
    }
    'listen_loop: loop {
        match socket.recv_from(&mut buf) {
            Ok((size, src)) => {
                let received = String::from_utf8(buf[..size].to_vec()).expect("");
                println!("{} : Received {} from {}", data.ip_adress, received, src);
            }
            Err(ref err) if err.kind() == std::io::ErrorKind::TimedOut => {
                if !data.stop_connection.is_empty() {
                    return Ok(());
                }
            }
            Err(_) => {}
        }
    }
    Ok(())
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
    let (f_tx, f_rx) = unbounded::<ThreadAction>();
    println!("Starting listener threads");
    let node2_data = ChildNode {
        divisor_value: 3,
        ip_adress: node2_address.to_owned(),
        parent_connection: n2_rx,
    };
    let node3_data = ChildNode {
        divisor_value: 5,
        ip_adress: node3_address.to_owned(),
        parent_connection: n3_rx,
    };
    let node2_handler = thread::spawn(move || listener_node_routine(node2_data));
    let node3_handler = thread::spawn(move || listener_node_routine(node3_data));
    println!("Starting master node");
    let node1_handler = thread::spawn(move || {
        master_node_routine(MasterNodeData {
            ip_adress: node1_address.to_owned(),
            stop_connection: n1_rx,
            listener_nodes: vec![node2_address.to_owned(), node3_address.to_owned()],
        })
    });
    thread::sleep(Duration::from_secs(5));
    println!("Times out, sending kill signals to threads");
    n2_tx.send(ThreadAction::STOP)?;
    n3_tx.send(ThreadAction::STOP)?;
    n1_tx.send(ThreadAction::STOP)?;
    println!("END");
    Ok(())
}
