//! Test-only transparent PostgreSQL wire fault: no protocol/auth bytes are retained.
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
pub struct ProtocolFault {
    pub port: u16,
    mode: Arc<AtomicU8>,
    signal: Option<oneshot::Receiver<()>>,
    listener: JoinHandle<()>,
    connections: Arc<Mutex<Vec<JoinHandle<()>>>>,
}
impl ProtocolFault {
    pub async fn new() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mode = Arc::new(AtomicU8::new(0));
        let (sender, signal) = oneshot::channel();
        let sender = Arc::new(Mutex::new(Some(sender)));
        let connections = Arc::new(Mutex::new(Vec::new()));
        let tasks = connections.clone();
        let state = mode.clone();
        let listener = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    break;
                };
                let mode = state.clone();
                let sender = sender.clone();
                let task = tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(("127.0.0.1", 55432)).await else {
                        return;
                    };
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let outbound = tokio::io::copy(&mut client_read, &mut server_write);
                    let inbound = async {
                        loop {
                            let mut header = [0_u8; 5];
                            if server_read.read_exact(&mut header).await.is_err() {
                                break;
                            }
                            let length =
                                u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
                            if !(4..=2_097_152).contains(&length) {
                                break;
                            }
                            let mut payload = vec![0; length - 4];
                            if server_read.read_exact(&mut payload).await.is_err() {
                                break;
                            }
                            let expected = match payload.as_slice() {
                                b"BEGIN\0" => 1,
                                b"COMMIT\0" => 2,
                                _ => 0,
                            };
                            if header[0] == b'C'
                                && expected != 0
                                && mode
                                    .compare_exchange(
                                        expected,
                                        0,
                                        Ordering::SeqCst,
                                        Ordering::SeqCst,
                                    )
                                    .is_ok()
                            {
                                if let Some(sender) = sender.lock().unwrap().take() {
                                    let _ = sender.send(());
                                }
                                if expected == 1 {
                                    std::future::pending::<()>().await;
                                }
                                break;
                            }
                            if client_write.write_all(&header).await.is_err()
                                || client_write.write_all(&payload).await.is_err()
                            {
                                break;
                            }
                        }
                    };
                    tokio::select! { _=outbound=>(),_=inbound=>() }
                });
                tasks.lock().unwrap().push(task);
            }
        });
        Self {
            port,
            mode,
            signal: Some(signal),
            listener,
            connections,
        }
    }
    pub fn hold_begin_ack(&self) {
        self.mode.store(1, Ordering::SeqCst);
    }
    pub fn drop_commit_ack(&self) {
        self.mode.store(2, Ordering::SeqCst);
    }
    pub async fn intercepted(&mut self) {
        tokio::time::timeout(Duration::from_secs(10), self.signal.take().unwrap())
            .await
            .unwrap()
            .unwrap();
    }
}
impl Drop for ProtocolFault {
    fn drop(&mut self) {
        self.listener.abort();
        for task in self.connections.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}
