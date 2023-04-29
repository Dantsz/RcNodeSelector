use arrayref::array_ref;
use crossbeam::channel::{unbounded, Receiver, SendError};
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use std::net::UdpSocket;
use std::thread::{self, Thread};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
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
    let timeout = Duration::from_secs(5);
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
                    break 'listen_loop;
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
    let timeout = Duration::from_secs(5);
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
                    break 'listen_loop;
                }
            }
            Err(_) => {}
        }
    }
    Ok(())
}
fn main() -> Result<(), SendError<ThreadAction>> {
    const NODE1_ADDRESS: &str = "127.0.0.4:1001";
    const NODE2_ADDRESS: &str = "127.0.0.2:1001";
    const NODE3_ADDRESS: &str = "127.0.0.3:1001";

    println!("Creating channels");
    let (n1_tx, n1_rx) = unbounded::<ThreadAction>();
    let (n2_tx, n2_rx) = unbounded::<ThreadAction>();
    let (n3_tx, n3_rx) = unbounded::<ThreadAction>();
    let (f_tx, f_rx) = unbounded::<ThreadAction>();
    println!("Starting listener threads");
    let node2_data = ChildNode {
        divisor_value: 3,
        ip_adress: NODE2_ADDRESS.to_owned(),
        parent_connection: n2_rx,
    };
    let node3_data = ChildNode {
        divisor_value: 5,
        ip_adress: NODE3_ADDRESS.to_owned(),
        parent_connection: n3_rx,
    };
    println!("Starting Main Child Nodes");
    let _node2_handler = thread::spawn(move || listener_node_routine(node2_data));
    let _node3_handler = thread::spawn(move || listener_node_routine(node3_data));
    println!("Starting master node");
    let _master_listener_thread = thread::spawn(move || {
        master_node_routine(MasterNodeData {
            ip_adress: NODE1_ADDRESS.to_owned(),
            stop_connection: n1_rx,
            listener_nodes: vec![NODE2_ADDRESS.to_owned(), NODE3_ADDRESS.to_owned()],
        })
    });
    thread::sleep(Duration::from_secs(5));
    println!("Times out, sending kill signals to threads");
    n1_tx.send(ThreadAction::STOP)?;
    n2_tx.send(ThreadAction::STOP)?;
    n3_tx.send(ThreadAction::STOP)?;
    println!("END");
    Ok(())
}
