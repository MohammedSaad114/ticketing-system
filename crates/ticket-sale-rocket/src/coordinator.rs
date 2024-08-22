use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use ticket_sale_core::Config;
use uuid::Uuid;

use crate::database::Database;
use crate::messages::{CoordinatorMessage, ServerMessage, ServerOrRequestMessage};
use crate::server::Server;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerState {
    Running,
    Terminating,
    HasStopped,
}

type ServerMap = Arc<
    RwLock<
        HashMap<
            Uuid,
            (
                Sender<ServerOrRequestMessage>,
                Arc<Mutex<ServerState>>,
                Option<JoinHandle<()>>,
            ),
        >,
    >,
>;

type TerminatingServersHandles = Arc<RwLock<Vec<(Uuid, Option<JoinHandle<()>>)>>>;

pub struct Coordinator {
    database: Arc<RwLock<Database>>,
    servers: ServerMap,
    running: AtomicBool,
    terminating_handles: TerminatingServersHandles,
    message_tx: Sender<CoordinatorMessage>,
    reservation_timeout: u32,
}

impl Coordinator {
    pub fn new(
        config: &Config,
        database: Arc<RwLock<Database>>,
        message_tx: Sender<CoordinatorMessage>,
    ) -> Self {
        let mut servers = HashMap::new();
        for _ in 0..config.initial_servers {
            let (server, sender, server_state, handle) =
                Self::spawn_server(database.clone(), config.timeout, message_tx.clone());
            servers.insert(server.read().unwrap().id(), (sender, server_state, handle));
        }

        Self {
            database,
            servers: RwLock::new(servers).into(),
            running: AtomicBool::new(true),
            message_tx,
            reservation_timeout: config.timeout,
            terminating_handles: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn spawn_server(
        database: Arc<RwLock<Database>>,
        reservation_timeout: u32,
        coordinator_tx: Sender<CoordinatorMessage>,
    ) -> (
        Arc<RwLock<Server>>,
        Sender<ServerOrRequestMessage>,
        Arc<Mutex<ServerState>>,
        Option<JoinHandle<()>>,
    ) {
        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let server_state = Arc::new(Mutex::new(ServerState::Running));

        let cloned_tx = tx.clone();
        let server: Arc<RwLock<Server>> = Arc::new(RwLock::new(Server::new(
            database,
            coordinator_tx,
            5,
            reservation_timeout,
            rx,
            server_state.clone(),
            cloned_tx,
        )));

        let server_clone = Arc::clone(&server);
        let handle = thread::spawn(move || {
            let mut server = server_clone.write().unwrap();
            server.run();
        });

        (server, tx, server_state, Some(handle))
    }

    pub fn set_num_servers(&self, num_servers: usize) {
        let mut servers = self.servers.write().unwrap();

        match num_servers.cmp(&servers.len()) {
            std::cmp::Ordering::Greater => {
                for _ in servers.len()..num_servers {
                    let (server, sender, server_state, handle) = Self::spawn_server(
                        self.database.clone(),
                        self.reservation_timeout,
                        self.message_tx.clone(),
                    );
                    servers.insert(server.read().unwrap().id(), (sender, server_state, handle));
                }
            }
            std::cmp::Ordering::Less => {
                let server_ids: Vec<Uuid> = servers.keys().cloned().collect();
                for server_id in server_ids.iter().skip(num_servers) {
                    if let Some((sender, server_state, handle)) = servers.remove(server_id) {
                        {
                            let mut state = server_state.lock().unwrap();
                            *state = ServerState::Terminating;
                        }

                        sender
                            .send(ServerOrRequestMessage::ServerMessage(
                                ServerMessage::TerminateServer,
                            ))
                            .unwrap_or_else(|e| {
                                eprintln!(
                                    "Failed to send termination message to server {}: {}",
                                    server_id, e
                                );
                            });

                        let mut terminating_handles = self.terminating_handles.write().unwrap();
                        terminating_handles.push((*server_id, handle));
                    }
                }
            }
            _ => {}
        }
    }

    pub fn get_running_servers(&self) -> Vec<Uuid> {
        self.servers
            .read()
            .unwrap()
            .iter()
            .filter_map(|(server_id, (_, server_state, _))| {
                if let ServerState::Running = *server_state.lock().unwrap() {
                    Some(*server_id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_servers(&self) -> Vec<Uuid> {
        self.servers.read().unwrap().keys().cloned().collect()
    }

    pub fn get_server_sender(&self, server_id: Uuid) -> Option<Sender<ServerOrRequestMessage>> {
        self.servers
            .read()
            .unwrap()
            .get(&server_id)
            .map(|(sender, _, _)| sender.clone())
    }

    pub fn run(&self, rx: Receiver<CoordinatorMessage>) {
        while self.running.load(Ordering::SeqCst) {
            if let Ok(message) = rx.recv_timeout(Duration::from_millis(100)) {
                match message {
                    CoordinatorMessage::GetNumServers(sender) => {
                        let num_servers = self.get_running_servers().len() as u32;
                        sender.send(num_servers).unwrap_or_else(|e| {
                            eprintln!("Failed to send number of servers: {}", e)
                        });
                    }
                    CoordinatorMessage::SetNumServers(num, sender) => {
                        self.set_num_servers(num);
                        let actual_num_servers = self.get_running_servers().len() as u32;
                        sender.send(actual_num_servers).unwrap_or_else(|e| {
                            eprintln!("Failed to send set server response: {}", e)
                        });
                    }
                    CoordinatorMessage::GetServers(sender) => {
                        let server_ids = self.get_running_servers();
                        sender
                            .send(server_ids)
                            .unwrap_or_else(|e| eprintln!("Failed to send server list: {}", e));
                    }
                    CoordinatorMessage::GetServerSender(server_id, sender) => {
                        if let Some(server_sender) = self.get_server_sender(server_id) {
                            sender.send(server_sender).unwrap_or_else(|e| {
                                eprintln!("Failed to send server sender: {}", e)
                            });
                        }
                    }
                    CoordinatorMessage::Shutdown => {
                        self.shutdown();
                        break;
                    }
                    CoordinatorMessage::ServerTerminated(server_id) => {
                        // Remove the server from the list
                        let mut servers = self.servers.write().unwrap();
                        if let Some((_, _, handle)) = servers.remove(&server_id) {
                            println!("Server {} has terminated and been removed.", server_id);

                            // Join the handle if it exists
                            if let Some(handle) = handle {
                                handle.join().unwrap_or_else(|e| {
                                    eprintln!(
                                        "Failed to join thread for server {}: {:?}",
                                        server_id, e
                                    );
                                });
                            }
                        }
                        self.cleanup_terminated_servers();
                    }
                }
            }
        }
    }

    fn cleanup_terminated_servers(&self) {
        let mut servers = self.servers.write().unwrap();
        let server_ids: Vec<Uuid> = servers.keys().cloned().collect();

        for server_id in server_ids {
            if let Some((_, server_state, _)) = servers.get(&server_id) {
                if *server_state.lock().unwrap() == ServerState::HasStopped {
                    servers.remove(&server_id);
                    println!(
                        "Server {} has fully terminated and been removed.",
                        server_id
                    );
                }
            }
        }
    }

    pub fn is_server_terminating(&self, id: Uuid) -> bool {
        self.servers.read().unwrap().get(&id).is_none()
    }

    pub fn get_message_tx(&self) -> Sender<CoordinatorMessage> {
        self.message_tx.clone()
    }

    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        println!("Coordinator is shutting down");

        let mut servers = self.servers.write().unwrap();

        for (server_id, (sender, _, handle)) in servers.drain() {
            println!("Sending shutdown message to Server {}", server_id);
            sender
                .send(ServerOrRequestMessage::ServerMessage(
                    ServerMessage::ShutdownServer,
                ))
                .unwrap_or_else(|e| {
                    eprintln!(
                        "Failed to send shutdown message to server {}: {}",
                        server_id, e
                    );
                });
            if let Some(handle) = handle {
                handle.join().unwrap_or_else(|e| {
                    eprintln!("Failed to join thread for server {}: {:?}", server_id, e);
                });
            }
            println!("Joined thread for Server {}", server_id);
        }

        println!("Coordinator has been fully shut down");
    }
}
